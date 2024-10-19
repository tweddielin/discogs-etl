from lxml import etree
import pyarrow as pa
import io

def create_arrays_from_chunk(chunk, schema):
    arrays = {}
    for field in schema:
        if pa.types.is_list(field.type):
            # Handle list types (like artists, genres, styles, images, videos)
            item_type = field.type.value_type
            if pa.types.is_struct(item_type):
                # For lists of structs (like artists, images, videos)
                arrays[field.name] = pa.array([r[field.name] for r in chunk], type=field.type)
            else:
                # For simple lists (like genres, styles)
                arrays[field.name] = pa.array([r[field.name] for r in chunk])
        elif pa.types.is_integer(field.type):
            # Handle integer types
            arrays[field.name] = pa.array([r[field.name] for r in chunk], type=field.type)
        else:
            # Handle other types (like strings)
            arrays[field.name] = pa.array([r[field.name] for r in chunk])
    return arrays

class XMLParser(object):
    def __init__(self, data_type):
        self.data_type = data_type

    def _parse_labels_data(self, elem):
        record = {
            'id': int(elem.findtext('id') or 0),
            'name': elem.findtext('name'),
            'contactinfo': elem.findtext('contactinfo'),
            'profile': elem.findtext('profile'),
            'data_quality': elem.findtext('data_quality'),
            'images': [],
            'urls': [],
            'sublabels': []
        }
        
        for image in elem.findall('.//images/image'):
            record['images'].append({
                'width': int(image.get('width') or 0),
                'height': int(image.get('height') or 0),
                'type': image.get('type'),
                'uri': image.get('uri'),
                'uri150': image.get('uri150')
            })
        
        record['urls'] = [url.text for url in elem.findall('.//urls/url')]
        record['sublabels'] = [sublabel.text for sublabel in elem.findall('.//sublabels/label')]
        return record

    def _parse_masters_data(self, elem):
        record = {
            'id': int(elem.get('id')),
            'main_release': int(elem.findtext('main_release') or 0),
            'artists': [],
            'genres': [],
            'styles': [],
            'year': int(elem.findtext('year') or 0),
            'title': elem.findtext('title'),
            'data_quality': elem.findtext('data_quality'),
            'images': [],
            'videos': []
        }
        
        for artist in elem.findall('.//artists/artist'):
            record['artists'].append({
                'id': int(artist.findtext('id') or 0),
                'name': artist.findtext('name'),
                'anv': artist.findtext('anv'),
                'join': artist.findtext('join'),
                'role': artist.findtext('role'),
                'tracks': artist.findtext('tracks')
            })
        
        record['genres'] = [genre.text for genre in elem.findall('.//genres/genre')]
        record['styles'] = [style.text for style in elem.findall('.//styles/style')]
        
        for image in elem.findall('.//images/image'):
            record['images'].append({
                'height': int(image.get('height') or 0),
                'width': int(image.get('width') or 0),
                'type': image.get('type'),
                'uri': image.get('uri'),
                'uri150': image.get('uri150')
            })
        
        for video in elem.findall('.//videos/video'):
            record['videos'].append({
                'duration': int(video.get('duration') or 0),
                'embed': video.get('embed') == 'true',
                'src': video.get('src'),
                'title': video.findtext('title'),
                'description': video.findtext('description')
            })
        return record

    def _parse_releases_data(self, elem):
        record = {
            'id': int(elem.get('id') or 0),
            'status': elem.get('status'),
            'title': elem.findtext('title'),
            'country': elem.findtext('country'),
            'released': elem.findtext('released'),
            'notes': elem.findtext('notes'),
            'images': [],
            'artists': [],
            'labels': [],
            'formats': [],
            'genres': [],
            'styles': []
        }
        
        for image in elem.findall('.//images/image'):
            record['images'].append({
                'height': int(image.get('height') or 0),
                'width': int(image.get('width') or 0),
                'type': image.get('type'),
                'uri': image.get('uri'),
                'uri150': image.get('uri150')
            })
        
        for artist in elem.findall('.//artists/artist'):
            record['artists'].append(artist.findtext('name'))
        
        for label in elem.findall('.//labels/label'):
            record['labels'].append({
                'name': label.get('name'),
                'catno': label.get('catno')
            })
        
        for format in elem.findall('.//formats/format'):
            format_record = {
                'name': format.get('name'),
                'qty': int(format.get('qty') or 1),
                'descriptions': [desc.text for desc in format.findall('.//description')]
            }
            record['formats'].append(format_record)
        
        record['genres'] = [genre.text for genre in elem.findall('.//genres/genre')]
        record['styles'] = [style.text for style in elem.findall('.//styles/style')]
        return record

    def _parse_artists_data(self, elem):
        record = {
            'id': int(elem.findtext('id') or 0),
            'name': elem.findtext('name'),
            'realname': elem.findtext('realname'),
            'profile': elem.findtext('profile'),
            'data_quality': elem.findtext('data_quality'),
            'urls': [url.text for url in elem.findall('.//urls/url')],
            'namevariations': [name.text for name in elem.findall('.//namevariations/name')],
            'aliases': [name.text for name in elem.findall('.//aliases/name')],
            'groups': [name.text for name in elem.findall('.//groups/name')],
            'members': [name.text for name in elem.findall('.//members/name')],
            'images': []
        }
        
        for image in elem.findall('.//images/image'):
            record['images'].append({
                'height': int(image.get('height') or 0),
                'width': int(image.get('width') or 0),
                'type': image.get('type'),
                'uri': image.get('uri'),
                'uri150': image.get('uri150')
            })
        return record
        
    def parse_element(self, elem):
        if self.data_type == "master":
            return self._parse_masters_data(elem)
        elif self.data_type == "label":
            return self._parse_labels_data(elem)
        elif self.data_type == "release":
            return self._parse_releases_data(elem)
        elif self.data_type == "artist":
            return self._parse_artists_data(elem)
        else:
            raise NotImplementedError(f"The parse method for data_type {self.data_type} is not implemented.")
    
def parse_element(elem):
    data = {}
    for child in elem:
        if child.tag == 'images':
            data[child.tag] = []
            for image in child:
                img_data = {attr: image.get(attr) for attr in image.attrib if image.get(attr)}
                if img_data:
                    data[child.tag].append(img_data)
        elif child.tag == 'videos':
            data[child.tag] = []
            for video in child:
                video_data = {attr: video.get(attr) for attr in video.attrib if video.get(attr)}
                title_description = {i.tag: i.text for i in video}
                video_data.update(title_description)
                if video_data:
                    data[child.tag].append(video_data)
        elif child.tag == 'urls':
            data[child.tag] = [url.text for url in child if url.text]
        elif child.tag == 'sublabels':
            data[child.tag] = [label.text for label in child if label.text]
        elif len(child) == 0:  # Text content
            if child.text:
                data[child.tag] = child.text.strip()
        else:  # Nested elements
            nested_data = parse_element(child)
            if nested_data:
                data[child.tag] = nested_data
    return data