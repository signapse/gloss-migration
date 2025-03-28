import os
import boto3
from sqlalchemy import create_engine, text
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = {
    'DB_CONNECTION': os.environ.get('DATABASE_URL'),
    'GLOSS_INPUTS_BUCKET_NAME': os.environ.get('GLOSS_INPUTS_BUCKET_NAME', 'dev-hub-gloss-inputs-s3'),
    'AWS_REGION_NAME': os.environ.get('AWS_REGION_NAME', 'eu-west-2'),
}

logger.info(f"Config: {config}")

# AWS Configuration
s3_client = boto3.client('s3',
    region_name=config['AWS_REGION_NAME'],
)

def generateFileKeyForGlossVideo(fileKey: str):
    return f"inputs/Data_Videos/{fileKey}"

def generateFileNameForGlossVideo(glossText: str):
    return f"{glossText}.mp4"

def sync_video_names():
    # Create database engine
    engine = create_engine(config['DB_CONNECTION'], echo=True)  # Enables query logging

    try:
        with engine.connect() as connection:
            # Get all records from your table
            logger.info("Getting all records from gloss_dictionary")

            result = connection.execute(text("SELECT id, text, video_file_name FROM gloss_dictionary"))
            rows = result.fetchall()

            logger.info(f"Rows Count: {len(rows)}")

            for row in rows:
                if row.video_file_name is None:
                    logger.info(f"Skipping record as no video file name is set {row.id}")
                    continue

                video_name_without_ext = row.video_file_name.replace('.mp4', '')

                if row.text != video_name_without_ext:
                    success = process_record(row, connection)
                    if success:
                        logger.info(f"✓ Successfully processed {row.video_file_name}")
                    else:
                        logger.warning(f"⚠️ Skipping record {row.id}: {row.video_file_name}")
                else:
                    logger.info(f"✓ Skipping record {row.id}: {row.video_file_name}")

    except Exception as e:
        logger.error(f"Database error: {str(e)}")

def process_record(row, connection):
    try:
        source_key = generateFileKeyForGlossVideo(row.video_file_name)
        target_key = generateFileKeyForGlossVideo(generateFileNameForGlossVideo(row.text))

        logger.info(f"Source key: {source_key}")
        logger.info(f"Target key: {target_key}")
        
        # 1. First verify source exists
        try:
            s3_client.head_object(
                Bucket=config['GLOSS_INPUTS_BUCKET_NAME'],
                Key=source_key
            )
            logging.info(f"✓ Source file exists: {source_key}")
        except Exception as e:
            logging.error(f"❌ Source file missing: {source_key}. Error: {e}")
            return False

        # 2. Copy file
        s3_client.copy_object(
            Bucket=config['GLOSS_INPUTS_BUCKET_NAME'],
            CopySource=f"{config['GLOSS_INPUTS_BUCKET_NAME']}/{source_key}",
            Key=target_key
        )
        logging.info(f"✓ Copied to: {target_key}")

        # 3. Verify copy succeeded
        s3_client.head_object(
            Bucket=config['GLOSS_INPUTS_BUCKET_NAME'],
            Key=target_key
        )
        logging.info(f"✓ Verified copy exists")

        # 4. Only then delete original
        s3_client.delete_object(
            Bucket=config['GLOSS_INPUTS_BUCKET_NAME'],
            Key=source_key
        )
        logging.info(f"✓ Deleted original: {source_key}")

        # Update database with new filename
        connection.execute(
            text("UPDATE gloss_dictionary SET video_file_name = :new_name WHERE id = :id"),
            {"new_name": generateFileNameForGlossVideo(row.text), "id": row.id}
        )
        connection.commit()
        logger.info(f"✓ Successfully updated record {row.id}")
        
        return True

    except Exception as e:
        logging.error(f"❌ Failed processing {row.video_file_name}: {e}")
        return False

if __name__ == "__main__":
    try:
        sync_video_names()
        logger.info("Sync completed")
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
