import re
import unicodedata

from dateutil.parser import parse


class CleanUtils:

    MASTER_DICT = {
        'status': ['Movimiento', 'ESTATUS', 'STATUS'],
        '_id': ['ID', 'TICKET'],
        'name': ['Nombre', 'NOMBRE', 'QTY Recursos'],
        'address': ['Direccion'],
        'suburb': ['Colonia'],
        'postal_code': ['CP'],
        'city': ['Ciudad'],
        'state': ['Estado'],
        'service': ['Servicio', 'SERVICIO', 'TIPO_SERVICIO'],
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
        'region': ['Region', 'REGION'],
        'service_time': ['Horario_Atencion'],
        'longitude': [],
        'latitude': [],
        'responsable': ['Atiende'],
        'start_date': ['FECHA_INICIO', 'FECHA INICIO', 'FECHA_CREACION'],
        'end_date': ['FECHA_FIN', 'FECHA FIN', ' FECHA_CIERRE'],
        'last_status_update_date': ['FECHA_ULTIMO_STATUS'],
        'failure': ['FALLA', 'DESCRIPCION'],
        'failure_type': ['TIPO FALLA'],
        'time_used': ['DURACION_ORIGINAL', 'DURA ORIG', 'DURACION ORIGINAL'],
        'accomplish': ['CUMPLIMIENTO'],
        'time_limited': ['T_A', "TA"],
        'time_granted': ['24 HRS GRACIA', 'TIEMPO A RETIPI'],
        'time_charged': ['DURACION NA', 'DUR - RETIS', 'DURACION TOTAL'],
        'platform': ['Plataforma'],
        'manager': ['Field Manager'],
        'go_to_atm': ['Pasa a Cajeros'],
        'severity': ['SEVERIDAD'],
        'engineer_id': ['EMPLOYEE_NBR'],
        'engineer_name': ['EMPLOYEE_NAME'],
        'bu_code': ['BU_CODE'],
        'call_comments': ['CALL_COMMENTS'],
        'creator_id': ['CREATION_USER_ID'],
        'atm_mvs': ['ATM_MVS'],
        'client_id': ['CLI_NUMERO']
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
        return cleaned.strip()

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
    def standarize_keys_dict(key_list, to_alpha=True):
        ''' Translate summary keys to standard keys
        :param summary: A summary dictionary
        :returns: Same dictionary with keys standardized
        '''
        new = {}
        for key in key_list:
            for real_key, keywords in CleanUtils.MASTER_DICT.items():
                if CleanUtils.clean_string(key, to_alpha=to_alpha, lower=False) in keywords:
                    new[key] = real_key
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
        except (ValueError, TypeError) as e:
            print(e)
            pass

        return date
