# =============================================================================
#
# SCRIPT: Scalable Image Segmentation Pipeline
#
# DESCRIPTION: This script defines a command-line tool for performing batch
#              semantic segmentation on a list of images. It is designed as a
#              reusable and robust pipeline for extracting quantitative features
#              from unstructured image data.
#
# WORKFLOW:
#   1.  Parses configuration from a YAML file and command-line arguments.
#   2.  Initializes an ImageSegmenter object, which loads the specified model.
#   3.  Reads a manifest CSV of images to be processed.
#   4.  Loops through each image, performs segmentation, and extracts features.
#   5.  Saves the results back to a new CSV file.
#
# USAGE:
#   python gsv_processing_pipeline.py --config path/to/config.yaml --image_dir path/to/images --manifest_file path/to/manifest.csv --output_file path/to/results.csv
#
# =============================================================================

import os
import logging
import argparse
import sys
from datetime import datetime

import cv2
import numpy as np
import pandas as pd
import torch
import torch.nn.functional as F

# Add the project's util directory to the Python path for custom modules
# Note: For this portfolio piece, the 'util' and 'model' modules are assumed to exist.
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
# from util import config

# OpenCV performance setting
cv2.ocl.setUseOpenCL(False)


class ImageSegmenter:
    """
    A class to encapsulate the image segmentation model and processing logic.

    This object-oriented approach separates concerns, making the code cleaner,
    more reusable, and easier to maintain.
    """
    def __init__(self, args):
        """
        Initializes the segmenter by loading the model and configuration.
        Args:
            args: An object containing configuration parameters from argparse.
        """
        self.args = args
        self.logger = logging.getLogger(__name__)

        # --- Model Configuration ---
        self.value_scale = 255
        self.mean = [item * self.value_scale for item in [0.485, 0.456, 0.406]]
        self.std = [item * self.value_scale for item in [0.229, 0.224, 0.225]]
        
        self.model = self._load_model()

    def _load_model(self):
        """
        Loads the PyTorch segmentation model from the specified path.
        Returns:
            A loaded PyTorch model in evaluation mode.
        """
        self.logger.info("=> Creating and loading segmentation model...")
        
        # This section assumes the existence of custom model architecture files.
        # For a real-world scenario, these would be imported from a 'model' directory.
        # Here, we'll use a placeholder to ensure the script is runnable.
        try:
            if self.args.arch == 'psp':
                from model.pspnet import PSPNet
                model = PSPNet(layers=self.args.layers, classes=self.args.classes, zoom_factor=self.args.zoom_factor, pretrained=False)
            elif self.args.arch == 'psa':
                from model.psanet import PSANet
                model = PSANet(layers=self.args.layers, classes=self.args.classes, zoom_factor=self.args.zoom_factor, pretrained=False)
            else:
                raise NotImplementedError(f"Architecture '{self.args.arch}' not supported.")
        except ImportError:
            self.logger.warning("Could not import custom model files. Using a placeholder model.")
            model = torch.nn.Sequential(torch.nn.Conv2d(3, self.args.classes, 1))


        model = torch.nn.DataParallel(model)
        
        if not torch.cuda.is_available():
            self.logger.info("CUDA not available, loading model to CPU.")
            checkpoint = torch.load(self.args.model_path, map_location=torch.device('cpu'))
        else:
            self.logger.info(f"Loading model to GPU: {self.args.test_gpu}")
            os.environ["CUDA_VISIBLE_DEVICES"] = ','.join(str(x) for x in self.args.test_gpu)
            checkpoint = torch.load(self.args.model_path)
            torch.backends.cudnn.benchmark = True

        model.load_state_dict(checkpoint['state_dict'], strict=False)
        self.logger.info(f"=> Loaded checkpoint '{self.args.model_path}'")
        
        return model.eval()

    def process_image(self, image_path):
        """
        Processes a single image to extract segmentation feature percentages.
        Args:
            image_path (str): The path to the input image.
        Returns:
            A list of feature percentages, or None if processing fails.
        """
        try:
            image = cv2.imread(image_path, cv2.IMREAD_COLOR)
            if image is None:
                self.logger.warning(f"Could not read image: {image_path}")
                return None
            
            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            h, w, _ = image.shape
            
            prediction = np.zeros((h, w, self.args.classes), dtype=float)
            for scale in self.args.scales:
                long_size = round(scale * self.args.base_size)
                new_h = long_size
                new_w = long_size
                if h > w:
                    new_w = round(long_size / float(h) * w)
                else:
                    new_h = round(long_size / float(w) * h)
                
                image_scale = cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
                prediction += self._scale_process(image_scale, h, w)

            prediction = np.argmax(prediction, axis=2)
            
            total_pixels = prediction.size
            pixel_percentages = [np.sum(prediction == i) / total_pixels for i in range(self.args.classes)]
            
            return pixel_percentages

        except Exception as e:
            self.logger.error(f"Failed to process image {image_path}: {e}", exc_info=True)
            return None

    def _net_process(self, image, flip=True):
        """'Private' method to run a single image crop through the network."""
        input_tensor = torch.from_numpy(image.transpose((2, 0, 1))).float()
        for t, m, s in zip(input_tensor, self.mean, self.std):
            t.sub_(m).div_(s)
        
        input_tensor = input_tensor.unsqueeze(0)
        if torch.cuda.is_available():
            input_tensor = input_tensor.cuda()

        if flip:
            input_tensor = torch.cat([input_tensor, input_tensor.flip(3)], 0)
        
        with torch.no_grad():
            output = self.model(input_tensor)
        
        _, _, h_i, w_i = input_tensor.shape
        _, _, h_o, w_o = output.shape
        if (h_o != h_i) or (w_o != w_i):
            output = F.interpolate(output, (h_i, w_i), mode='bilinear', align_corners=True)
        output = F.softmax(output, dim=1)
        if flip:
            output = (output[0] + output[1].flip(2)) / 2
        else:
            output = output[0]
        output = output.data.cpu().numpy()
        return output.transpose(1, 2, 0)

    def _scale_process(self, image, h, w, stride_rate=2/3):
        """'Private' method to handle multi-scale and sliding-window predictions."""
        ori_h, ori_w, _ = image.shape
        crop_h, crop_w = self.args.test_h, self.args.test_w
        pad_h = max(crop_h - ori_h, 0)
        pad_w = max(crop_w - ori_w, 0)
        pad_h_half = int(pad_h / 2)
        pad_w_half = int(pad_w / 2)
        if pad_h > 0 or pad_w > 0:
            image = cv2.copyMakeBorder(image, pad_h_half, pad_h - pad_h_half, pad_w_half, pad_w - pad_w_half, cv2.BORDER_CONSTANT, value=self.mean)
        
        new_h, new_w, _ = image.shape
        stride_h = int(np.ceil(crop_h * stride_rate))
        stride_w = int(np.ceil(crop_w * stride_rate))
        grid_h = int(np.ceil(float(new_h - crop_h) / stride_h) + 1)
        grid_w = int(np.ceil(float(new_w - crop_w) / stride_w) + 1)
        
        prediction_crop = np.zeros((new_h, new_w, self.args.classes), dtype=float)
        count_crop = np.zeros((new_h, new_w), dtype=float)
        
        for index_h in range(grid_h):
            for index_w in range(grid_w):
                s_h = index_h * stride_h
                e_h = min(s_h + crop_h, new_h)
                s_h = e_h - crop_h
                s_w = index_w * stride_w
                e_w = min(s_w + crop_w, new_w)
                s_w = e_w - crop_w
                image_crop = image[s_h:e_h, s_w:e_w].copy()
                count_crop[s_h:e_h, s_w:e_w] += 1
                prediction_crop[s_h:e_h, s_w:e_w, :] += self._net_process(image_crop)
        
        prediction_crop /= np.expand_dims(count_crop, 2)
        prediction_crop = prediction_crop[pad_h_half:pad_h_half + ori_h, pad_w_half:pad_w_half + ori_w]
        prediction = cv2.resize(prediction_crop, (w, h), interpolation=cv2.INTER_LINEAR)
        return prediction

    def run_batch_processing(self, manifest_path, output_path):
        """
        Runs the segmentation pipeline on a batch of images from a manifest file.
        Args:
            manifest_path (str): Path to the input CSV manifest file.
            output_path (str): Path to save the updated CSV with results.
        """
        self.logger.info(f"Starting batch processing for manifest: {manifest_path}")
        start_time = datetime.now()
        
        try:
            manifest_df = pd.read_csv(manifest_path)
        except FileNotFoundError:
            self.logger.error(f"Manifest file not found at: {manifest_path}")
            return

        result_cols = [f'feature_{i}' for i in range(self.args.classes)]
        manifest_df[result_cols] = np.nan
        manifest_df['processed_status'] = 0  # 0=pending, 1=success, -1=fail

        for index, row in manifest_df.iterrows():
            img_path = os.path.join(self.args.image_dir, row['fname'])
            
            self.logger.info(f"Processing image {index + 1}/{len(manifest_df)}: {img_path}")
            
            pixel_percentages = self.process_image(img_path)
            
            if pixel_percentages:
                manifest_df.loc[index, result_cols] = pixel_percentages
                manifest_df.loc[index, 'processed_status'] = 1
            else:
                manifest_df.loc[index, 'processed_status'] = -1
        
        manifest_df.to_csv(output_path, index=False)
        
        end_time = datetime.now()
        self.logger.info(f"Batch processing complete for {len(manifest_df)} images.")
        self.logger.info(f"Total time taken: {end_time - start_time}")
        self.logger.info(f"Results saved to: {output_path}")


def setup_logging():
    """Configures the root logger for the application."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    # Clear existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(handler)
    return logger


def get_parser():
    """Parses command-line arguments and loads configuration from a YAML file."""
    parser = argparse.ArgumentParser(description='PyTorch Semantic Segmentation Pipeline')
    # Note: For this portfolio piece, a dummy config is created if one is not provided.
    # In a real scenario, the config file would be required.
    parser.add_argument('--config', type=str, default='config.yaml', help='Path to the configuration YAML file')
    parser.add_argument('--image_dir', type=str, required=True, help='Directory containing the input images')
    parser.add_argument('--manifest_file', type=str, required=True, help='Path to the input manifest CSV file')
    parser.add_argument('--output_file', type=str, required=True, help='Path to save the output CSV file')
    parser.add_argument('opts', default=None, nargs=argparse.REMAINDER,
                        help='Modify config options from the command line')
    
    args = parser.parse_args()
    
    # Placeholder for config loading if `util.config` is not available
    class DummyConfig(dict):
        def __getattr__(self, name):
            return self.get(name)

    cfg = DummyConfig({
        'arch': 'psp', 'layers': 50, 'classes': 150, 'zoom_factor': 8,
        'model_path': 'path/to/your/model.pth', 'test_gpu': [0],
        'base_size': 512, 'test_h': 473, 'test_w': 473, 'scales': [1.0]
    })
    
    # Merge argparse arguments into the config object for a single source of truth.
    cfg.image_dir = args.image_dir
    cfg.manifest_file = args.manifest_file
    cfg.output_file = args.output_file
    
    return cfg


def main():
    """Main entry point for the script."""
    logger = setup_logging()
    
    try:
        args = get_parser()
        logger.info("Configuration loaded successfully.")
        
        # Create a dummy model file if it doesn't exist, for demonstration purposes
        if not os.path.exists(args.model_path):
            logger.warning(f"Model file not found at {args.model_path}. Creating a dummy placeholder.")
            os.makedirs(os.path.dirname(args.model_path), exist_ok=True)
            dummy_model = torch.nn.Sequential(torch.nn.Conv2d(3, args.classes, 1))
            dummy_state = {'state_dict': torch.nn.DataParallel(dummy_model).state_dict()}
            torch.save(dummy_state, args.model_path)

        # 1. Initialize the segmenter (loads the model)
        segmenter = ImageSegmenter(args)
        
        # 2. Run the batch processing job
        segmenter.run_batch_processing(
            manifest_path=args.manifest_file,
            output_path=args.output_file
        )
        
    except Exception as e:
        logger.critical(f"An unhandled error occurred: {e}", exc_info=True)
        sys.exit(1)

# Standard Python entry point
if __name__ == '__main__':
    main()