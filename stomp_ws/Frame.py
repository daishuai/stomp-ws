import logging

Byte = {
    'LF': '\x0A',
    'NULL': '\x00'
}


class Frame:

    def __init__(self, command, headers, body):
        self.command = command
        self.headers = headers
        self.body = '' if body is None else body

    def __str__(self):
        lines = [self.command]
        skip_content_length = 'content-length' in self.headers
        if skip_content_length:
            del self.headers['content-length']

        for name in self.headers:
            value = self.headers[name]
            lines.append("" + name + ":" + value)

        if self.body is not None and not skip_content_length:
            lines.append("content-length:" + str(len(self.body)))

        lines.append(Byte['LF'] + self.body)
        return Byte['LF'].join(lines)

    @staticmethod
    def unmarshall_single(data):
        if data == Byte['LF']:
            logging.info('Server Heartbeat!')
            return Frame('HEARTBEAT', {}, None)
        lines = data.split(Byte['LF'])
        command = lines[0].strip()
        headers = {}
        if len(lines) == 1:
            return Frame(command, headers, None)
        # get all headers
        i = 1
        while lines[i] != '':
            # get key, value from raw header
            (key, value) = lines[i].split(':')
            headers[key] = value
            i += 1

        # set body to None if there is no body
        body = None if lines[i + 1] == Byte['NULL'] else lines[i + 1][:-1]

        return Frame(command, headers, body)

    @staticmethod
    def marshall(command, headers, body):
        return str(Frame(command, headers, body)) + Byte['NULL']
