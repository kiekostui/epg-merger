import requests
import gzip
import xml.etree.ElementTree as ET
import os
from urllib.parse import urlparse
from datetime import datetime, timedelta, UTC
import sys


source_file='source_epg.txt' #File name with epg sources and channels
output_epg = 'epg_tv_it.xml' #Output epfg file name
TEMP_DIR_NAME = 'temp_epg_files'
output_xml = 'epg.xml'
default_time_frame = 48 #Defaul value if not present in the source file


######## parse source file ###################
def parse_source(source_file):
    with open(source_file, 'r') as source:

        time_frame_string = source.readline().rpartition('=')[2]
        try:
            time_frame = int(time_frame_string.strip())
            print(f'Timeframe: {time_frame}\n')
            
        except:
            time_frame = ''
        if not time_frame:
            time_frame = default_time_frame
            print('No timeframe valid string provided. Default value (48h) will be used\n')


        source.seek(0) #Reset file position

        current_source = ''
        data_source = {}

        for line in source:
            line = line.partition('#')[0].strip()

            if not line:
                continue
            
            if line.startswith('http'):
                current_source = line
                if current_source not in data_source.keys():
                    data_source[current_source] = []

            else:
               if  current_source:
                   id_channel = line
                   if not id_channel in data_source[current_source]:
                       data_source[current_source].append(id_channel)

    return (data_source, time_frame)


######### download file from web ###############
def download_file(url, path):
    
    file = os.path.split(url)[1]

    if not file:
        print(f'url {url} not valid')
        return ''

    print(f'Downloading: {url}')

    download_path = os.path.join(path, file)

    file_name, file_ext = os.path.splitext(file)

    i=1

    while True:
        if os.path.exists(download_path):
            download_path = os.path.join(path, file_name + ('(') + str(i) + (')') + file_ext)
            i = i + 1
        else:
            break

    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        print(f'Error during download file {url}: {e}')
        return ''

    try:
        with open(download_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=131072):
                f.write(chunk)
        print (f'File {download_path} successfully saved')
        return download_path
                
    except Exception as e:
        print(f'Error during file writing: {e}')
        return ''

    
########convert data string to datetime object #############
def convert_date(epg_format_date):
    try:
        date_object = datetime.strptime(epg_format_date, '%Y%m%d%H%M%S %z')
    except:
        return None
    
    return (date_object)


############ parse epg file and append infos #############
def process_epgsource(file_path, channel_to_process, channel_section_xml, programm_section_xml, start, time_frame):

    file = os.path.split(file_path)[1]
    dir_path = os.path.split(file_path)[0]

    file_name, file_ext = os.path.splitext(file)

    if file_path.endswith('gz'):
        file = file_name + '.xml'
        file_xml = os.path.join(dir_path, file)
        try:
            with gzip.open(file_path, 'rb') as input_file:
                with open(file_xml, 'wb') as output_file:
                    output_file.write(input_file.read())
            os.remove(file_path)
                    
        except Exception as e:
            print(f'It was not possible to extact info''s from {file_path}')
            return
    else:
        file_xml = file_path

    try: 
        tree = ET.parse(file_xml)
    except ET.ParseError:
        print(f'File {file_xml} not valid')
        return

    except Exception as e:
        print(f'Error during parsing {file_xml}')
        return

    channel_skipped = channel_to_process.copy()

    n_channels = 0
    
    for channel in tree.findall('channel'):
        if channel.attrib['id'] in channel_to_process:
            channel_section_xml.append(channel)
            channel_skipped.remove(channel.attrib['id'])
            n_channels = n_channels + 1

    n_programs = 0

    for programme in tree.findall('programme'):
        if programme.attrib['channel'] in channel_to_process:
            program_start = convert_date(programme.attrib['start'])
            program_stop = convert_date(programme.attrib['stop'])
            if program_start and program_stop:
                start_delta = (program_start - start).total_seconds()/3600
                stop_delta = (program_stop - start).total_seconds()/3600
                if start_delta < time_frame and stop_delta > 0:
                    programm_section_xml.append(programme)
                    n_programs = n_programs + 1
            else:
                programm_section_xml.append(programme)
                n_programs = n_programs + 1

    for channel in channel_skipped:
        print(f'Channel {channel} not found')

    print(f'Channels extracted: {n_channels}')
    print(f'Programs extracted: {n_programs}')

    return

########### Main code ######################

start = datetime.now(UTC)
start_iso = start.strftime('%Y-%m-%d %H:%M:%S')
print(f'Start: {start_iso}\n')

info_source, time_frame = parse_source(source_file)

channel_section_xml = []
programm_section_xml = []
temp_dir = os.path.relpath(TEMP_DIR_NAME)

#Temp directory creation (if not exists)
os.makedirs(temp_dir, exist_ok=True)

#Initial cleaning of temp directory   
for temp_file in os.listdir(temp_dir):
    try:
        temp_file_path = os.path.join(temp_dir, temp_file)
        os.remove(temp_file_path)
    except:
        print(f'file {temp_file} cannot be deleted')


processed_channels = []

for source, channel_list in info_source.items():
    file_path = download_file(source, temp_dir) #Ref to local file downloaded
    
    channel_to_process = []

    for channel_id in channel_list: #To avoid duplicated
        if channel_id not in processed_channels:
            channel_to_process.append(channel_id)
        else:
            print(f'Channel {channel_id} skipped: duplicated')
            
            
    if file_path:
        process_epgsource(file_path, channel_to_process, channel_section_xml, programm_section_xml, start, time_frame)
        processed_channels.extend(channel_to_process) #Add channels to processed channels only if origin file is valid

    print()
        
        
root = ET.Element('tv')

for channel in channel_section_xml:
    root.append(channel)

for programme in programm_section_xml:
    root.append(programme)

tree = ET.ElementTree(root)
ET.indent(tree, space='    ', level=0)

tree.write(output_xml, encoding='UTF-8', xml_declaration=True)

#Final cleaning of temp directory  

for temp_file in os.listdir(temp_dir):
    try:
        temp_file_path = os.path.join(temp_dir, temp_file)
        os.remove(temp_file_path)
    except:
        print(f'file {temp_file} cannot be deleted')

print('Temp directory cleaned\n')

end = datetime.now(UTC)

end_iso = end.strftime('%Y-%m-%d %H:%M:%S')

print(f'End: {end_iso}')
