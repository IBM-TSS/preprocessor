import re
import unicodedata

from dateutil.parser import parse


class CleanUtils:

    MASTER_DICT = {
        'status': ['Movimiento'],
        '_id': ['ID'],
        'name': ['Nombre'],
        'addres': ['Direccion'],
        'suburb': ['Colonia'],
        'postal_code': ['CP'],
        'city': ['Ciudad'],
        'state': ['Estado'],
        'service': ['Servicio'],
        'acquisition_date': ['Adquisicion'],
        'first_service_date': ['Alta'],
        'last_status_date': ['Fecha_Movimiento'],
        'brand': ['Marca'],
        'model': ['Modelo'],
        'serie': ['Serie'],
        'inventory': ['Inventario'],
        'generation': ['Generacion'],
        'equipment': ['Equipo'],
        'function': ['Funcion'],
        'load': ['Carga'],
        'pinpad': ['Lectora'],
        'screen': ['Monitor'],
        'processor': ['Procesador'],
        'cpu_speed': ['VelocidadCPU'],
        'memory_ram': ['Memoria'],
        'memory_rom': ['CapacidadHD'],
        'cartridges': ['Cartuchos'],
        'region': ['Region'],
        'service_time': ['Horario_Atencion'],
        'longitude': [],
        'latitude': [],
        'responsable': ['Atiende'],
    }

    @staticmethod
    def to_ascii(string):
        ''' Normalize a string by clearing all non-ascii characters.
        :param string: A unicode string.
        :returns: ascii string.
        '''
        if isinstance(string, str):
            return unicodedata.normalize(
                'NFD', string
            ).encode(
                'ascii', 'ignore'
            ).decode(
                'utf-8'
            )
        else:
            return string

    @staticmethod
    def clean_string(string, lower=True, max_length=None, keep_none=False,
                     all_whitespace=False, to_ascii=False, replace_newline=False,
                     to_alpha=False):
        """ Removes unnecesary spaces from the given string
        :param string: A string
        :param lower: Whether to use force lowercase or not.
        :max_length: Max length for the string before being truncated.
        :keep_none: If the string is None, it returns None instead of empty str.
        :all_whitespace: If True, it will replace ALL whitespace with a space,
        instead of only spaces.
        :to_ascii: If True, normalized unicode to ascii.
        :replace_newline: If the string contains new line characters,
        transforms them to spaces
        :returns: Cleaned string
        """
        if string is None and keep_none:
            return None
        string = string or ''
        if not isinstance(string, str):
            string = ''
        cleaned = re.sub(
            r'\s+', ' ', string).strip() if all_whitespace else re.sub(
                r' +', ' ', string).strip()
        if lower:
            cleaned = cleaned.lower()
        if max_length:
            cleaned = CleanUtils.truncate_string(cleaned, max_length)
        if to_ascii:
            cleaned = CleanUtils.to_ascii(cleaned)
        if replace_newline:
            cleaned = cleaned.replace('\n', ' ')
        if to_alpha:
            cleaned = re.sub(r'\W+', '', cleaned)
        return cleaned

    @staticmethod
    def translate_keys(summary):
        ''' Translate summary keys to standard keys
        :param summary: A summary dictionary
        :returns: Same dictionary with keys standardized
        '''
        new = {}
        for key in summary.keys():
            for real_key, keywords in CleanUtils.MASTER_DICT.items():
                if CleanUtils.clean_string(key, to_alpha=True, lower=False) in keywords:
                    new[real_key] = summary.get(key)
        return new

    @staticmethod
    def date_from_string(raw_date, append_tz=False):
        """ Parse string containing date into a tz-aware datetime object.
        :param raw_date: string containing date in a parsable format
        :param append_to_raw: string that gets added to raw_date to help parse
        :returns: tz-aware datetime object or None in case of parsing error
        """
        date = None
        try:
            if append_tz:
                tz_extended_date = raw_date + " 08:00:00 -0800"
                date = parse(tz_extended_date)
            else:
                date = parse(raw_date)
        except (ValueError, TypeError):
            pass
        return date
