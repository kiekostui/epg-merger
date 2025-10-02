import requests
import gzip
import xml.etree.ElementTree as ET
import os
from urllib.parse import urlparse
import time
import shutil  # Import per la copia di file locali
import logging

logging.basicConfig(filemode='w',
                    filename='epg_log.log',
                    format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )
logging.getLogger().setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)





TEMP_DIR = "temp_epg_files"

def download_file(url, filepath):
    """Scarica un file da un URL e misura il tempo, con timeout."""
    start_time = time.time()
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()

    
    except requests.exceptions.RequestException as e:
        end_time = time.time()
        download_time = end_time - start_time
        logger.error (f"Error during download of {url} ( after {download_time:.2f} seconds): {e}\n")
        return False

    try:
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        end_time = time.time()
        download_time = end_time - start_time
        logger.info(f"Download of {url} completed in {download_time:.2f} seconds.")
        return True
    
    except IOError as e:
        logger.error(f'Error in writing {url} data to file {filepath}: {e}')
        return False

def extract_channels_from_xml(xml_file, channels_to_extract, source):
    """Estrae le informazioni dei canali specificati da un file XML EPG e fornisce feedback con la sorgente."""
    
    tree = ET.parse(xml_file)
    root = tree.getroot()
    extracted_channels_data = []
    extracted_programmes = []
    processed_count = 0
    bar_length = 20


    logger.info(f'Processing: {xml_file}')
    
    channels_to_extract = set(channels_to_extract)
    for channel in root.findall('channel'):
        channel_id = channel.get('id')
        if channel_id in channels_to_extract:
            extracted_channels_data.append(channel)
            channels_to_extract.remove(channel_id)

    if channels_to_extract:
        infochannel = f'No info\'s found in {source} for channels:'
        for channel in channels_to_extract:
            infochannel = infochannel + f'\n                                  {channel}'
        logger.info(infochannel)


    for programme in root.findall('programme'):
        channel_attr = programme.get('channel')
        if channel_attr in [ch.get('id') for ch in extracted_channels_data]:
            extracted_programmes.append(programme)

    logger.info(f'Extracted data from {xml_file}')


    return extracted_channels_data, extracted_programmes



def process_source_file(source_file):
    """Processa il file source.txt per scaricare/copiare ed estrarre i dati EPG con feedback sequenziale."""
    all_channels = []
    all_programmes = []
    current_source = None

    # Create temp directory

    os.makedirs(TEMP_DIR, exist_ok=True)

    with open(source_file, 'r') as f:
        channels_list = []
        source_channel_list = []
        for line in f:
            line = line.strip()
            if not line:
                continue

            if line.startswith("http"):
                if current_source:
                    source_channel_list.append((f'{current_source}', channels_list[:]))
                    
                channels_list = []
                current_source = line  

            else:
                if current_source:
                    channels_list.append(line)
                continue
            
        if current_source:
            source_channel_list.append((f'{current_source}', channels_list[:]))


        for source in source_channel_list:
            if not source[1]:
                logger.info(f'No channels listed for source: {source[0]}\n')
                continue
            parsed_url = urlparse(source[0])
            filename = os.path.basename(parsed_url.path)
            local_base_path = os.path.join(TEMP_DIR, filename)

            local_file_gz = local_base_path if filename.endswith(".gz") else None
            local_file_xml = local_base_path if not filename.endswith(".gz") else os.path.splitext(local_base_path)[0] #+ ".xml"

            #logger.info(f'Download from {source[0]}')
            downloaded = download_file(source[0], local_file_gz if local_file_gz else local_file_xml)
            
            if downloaded:
                try:
                    if local_file_gz:
                        logger.info(f'Extract : {local_file_gz}')
                        with gzip.open(local_file_gz, 'rb') as gz_file, open(local_file_xml, 'wb') as out_file:
                            out_file.write(gz_file.read())
                        logger.info(f'File exctracted: {local_file_xml}')

                    #logger.info(f'Reading file {local_file_xml}')
                    extracted_channels, extracted_programmes = extract_channels_from_xml(local_file_xml, source[1], source[0])
                    logger.info(f'Reading completed. Exctracted channels: {len(extracted_channels)}. Extracted programs: {len(extracted_programmes)}')
                    all_channels.extend(extracted_channels)
                    all_programmes.extend(extracted_programmes)

                except Exception as e:
                    logger.error(f'File not valid. Not possible to extract info: {e}')

                if os.path.exists(local_file_gz):
                    os.remove(local_file_gz)

                if os.path.exists(local_file_xml):
                    os.remove(local_file_xml)

                logger.info('Temp file deleted\n')


        if os.path.exists(TEMP_DIR):
            for item in os.listdir(TEMP_DIR):
                item_path = os.path.join(TEMP_DIR, item)
                try:
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)  # Rimuove anche le sottodirectory

                except Exception as e:
                    logger.error(f"  Error during removing '{item_path}': {e}\n")

            logger.info(f"  Temp directory '{TEMP_DIR}' cleaning complete\n")  

    return all_channels, all_programmes


def create_epg_xml(channels, programmes, output_filename="epg.xml"):
    """Crea il file XML EPG finale."""
    tv = ET.Element('tv')
    for channel in channels:
        tv.append(channel)
    for programme in programmes:
        tv.append(programme)

    tree = ET.ElementTree(tv)
    ET.indent(tree, space="  ", level=0)
    tree.write(output_filename, encoding='UTF-8', xml_declaration=True)
    print(f"File EPG XML successfully created: {output_filename}")
    logger.info(f"File EPG XML successfully created: {output_filename}")

if __name__ == "__main__":
    
    source_file = 'source_epg.txt'

    logger.info('Start\n')

    if not os.path.exists(source_file):
        logger.error(f'File {source_file} not found')
        exit()
        
    extracted_channels, extracted_programmes = process_source_file(source_file)
    
    if extracted_channels:
        create_epg_xml(extracted_channels, extracted_programmes)
    else:
        logger.info("Not channels to exract.")
