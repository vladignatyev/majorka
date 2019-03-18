import subprocess


class MajorkaException(Exception): pass

class Majorka(object):
    def __init__(self, binpath, redis_url):
        self.binpath = binpath
        self.redis_url = redis_url

    def _build_args_for_new_offer(self, name, url):
        return [
            self.binpath,
            '--redis', self.redis_url,
            'offers',
            'create',
            '--name', name,
            '--url', url
        ]

    def _build_args_for_new_campaign(self, name, alias, offer_ids, optimize=True, hit_limit=50, slice=('zone',)):
        normal = [
            self.binpath,
            '--redis', self.redis_url,
            'campaigns',
            'create',
            '--alias', alias,
            '--name', name,
            '--offers'
        ] + map(str, list(offer_ids))

        if not optimize:
            return normal

        smart = [
            '--hit-limit', str(int(hit_limit)),
            '--slice', ' '.join(map(str, slice))
        ]
        return normal + smart

    def create_offer(self, name, url):
        args = self._build_args_for_new_offer(name, url)
        try:
            s = result = subprocess.check_output(args, stderr=subprocess.STDOUT)
            new_offer_id = int(s.split('Offer { id: "Offer:[', 1)[1].split(']',1)[0])
            return new_offer_id
        except subprocess.CalledProcessError as e:
            raise MajorkaException("Unable to create "
                                   "new offer `{name}` with CLI call:"
                                   "{args}\n"
                                   "The output was:\n{output}".format(name=name,
                                                                      args=' '.join(args),
                                                                      output=e.output.decode()))

    def create_campaign(self, name, alias, offer_ids, optimize=True, hit_limit=50, slice=('zone',)):
        args = self._build_args_for_new_campaign(name, alias, offer_ids, optimize, hit_limit, slice)
        try:
            s = result = subprocess.check_output(args, stderr=subprocess.STDOUT)
            return result
        except subprocess.CalledProcessError as e:
            raise MajorkaException("Unable to create "
                                   "new campaign `{name}` with CLI call:"
                                   "{args}\n"
                                   "The output was:\n{output}".format(name=name,
                                                                      args=' '.join(args),
                                                                      output=e.output.decode()))
