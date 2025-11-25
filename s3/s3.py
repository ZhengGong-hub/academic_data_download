import boto3
import os
from pathlib import Path
from typing import Optional, List, Union, Dict
import logging

from dotenv import load_dotenv

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler if no handlers exist
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)

class S3Handler:
    """Handler for S3 operations with proper error handling and logging."""
    
    def __init__(self, 
                 region_name: str = 'eu-central-1',
                 bucket_name: str = 'jlgzsharebucket',
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None):
        """
        Initialize S3 handler.
        
        Args:
            region_name: AWS region name
            bucket_name: S3 bucket name
            aws_access_key_id: AWS access key ID (optional, can use environment variables)
            aws_secret_access_key: AWS secret access key (optional, can use environment variables)
        """
        self.s3 = boto3.resource(
            service_name='s3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        self.bucket = self.s3.Bucket(bucket_name)
        logger.info(f"Successfully initialized S3 handler for bucket: {bucket_name}")

    def upload_file(self, 
                   local_path: Union[str, Path], 
                   s3_key: str) -> str:
        """
        Upload a single file to S3.
        
        Args:
            local_path: Path to the local file
            s3_key: Key to use in S3
            
        Returns:
            str: S3 key of the uploaded file
        """
        logger.info(f"Uploading {local_path} to S3 as {s3_key}")
        self.bucket.upload_file(str(local_path), s3_key)
        logger.info(f"Successfully uploaded {s3_key}")
        return s3_key

    def upload_folder(self, 
                     folder_path: Union[str, Path], 
                     s3_prefix: str) -> List[str]:
        """
        Upload all files from a folder to S3 individually.
        
        Args:
            folder_path: Path to the folder containing files to upload
            s3_prefix: Prefix to use for S3 keys
            
        Returns:
            List[str]: List of S3 keys for uploaded files
        """
        folder_path = Path(folder_path)
        uploaded_keys = []
        
        for local_path in folder_path.rglob('*'):
            if local_path.is_file():
                # Create S3 key maintaining folder structure
                rel_path = local_path.relative_to(folder_path)
                s3_key = f"{s3_prefix}/{rel_path}"
                
                # Upload file
                self.upload_file(local_path, s3_key)
                uploaded_keys.append(s3_key)
        
        logger.info(f"Successfully uploaded {len(uploaded_keys)} files")
        return uploaded_keys

    def download_file(self, 
                     s3_key: str, 
                     local_path: Union[str, Path]) -> Path:
        """
        Download a file from S3.
        
        Args:
            s3_key: Key of the file in S3
            local_path: Local path to save the file
            
        Returns:
            Path: Path to the downloaded file
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Downloading {s3_key} to {local_path}")
        self.bucket.download_file(s3_key, str(local_path))
        logger.info(f"Successfully downloaded {s3_key}")
        
        return local_path

    def download_folder(self,
                       s3_prefix: str,
                       local_dir: Union[str, Path]) -> List[Path]:
        """
        Download all files with a given prefix from S3.
        
        Args:
            s3_prefix: Prefix of files to download
            local_dir: Local directory to save files
            
        Returns:
            List[Path]: List of paths to downloaded files
        """
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded_files = []
        for obj in self.bucket.objects.filter(Prefix=s3_prefix):
            if not obj.key.endswith('/'):  # Skip directories
                local_path = local_dir / Path(obj.key).relative_to(s3_prefix)
                self.download_file(obj.key, local_path)
                downloaded_files.append(local_path)
        
        logger.info(f"Successfully downloaded {len(downloaded_files)} files")
        return downloaded_files

    def list_files(self, 
                  prefix: str = '') -> List[Dict[str, str]]:
        """
        List files in the bucket with a given prefix.
        
        Args:
            prefix: Prefix to filter files
            
        Returns:
            List[Dict[str, str]]: List of file information dictionaries
        """
        files = []
        for obj in self.bucket.objects.filter(Prefix=prefix):
            files.append({
                'key': obj.key,
                'size': obj.size,
                'last_modified': obj.last_modified
            })
        return files

    def delete_file(self, s3_key: str) -> None:
        """
        Delete a file from S3.
        
        Args:
            s3_key: Key of the file to delete
        """
        logger.info(f"Deleting {s3_key} from S3")
        self.bucket.delete_objects(Delete={'Objects': [{'Key': s3_key}]})
        logger.info(f"Successfully deleted {s3_key}")


# Example usage
if __name__ == '__main__':
    s3_handler = S3Handler(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    
    # Example: Upload a folder
    s3_handler.upload_file(
        local_path='ciqcoldcopy/data/et_ref/complete_info/us_et_ref_v2.csv',
        s3_key='fin_data/et_ref/complete_info/us_et_ref_v2.csv'
    )
    
    # # Example: Download a folder
    # s3_handler.download_folder(
    #     s3_prefix='batch_output',
    #     local_dir='/home/ubuntu/downloads'
    # )