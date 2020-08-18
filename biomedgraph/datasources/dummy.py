import string
import os

from datacroaker import RollingReleaseRemoteDataSource


class Dummy(RollingReleaseRemoteDataSource):


    def __init__(self, root_dir):
        """
        initialize datasource with a root directory
        :param root_dir: root directory path
        :type root_dir: str
        """
        super(Dummy, self).__init__(root_dir)

    def download_function(self, instance):
        """
        Download a specific version.

        :param version: The version.
        :type version: DataSourceVersion
        """

        filename = 'file.txt'
        filepath = os.path.join(instance.process_instance_dir, filename)

        with open(filepath, 'wt') as f:
            for i in list(string.ascii_lowercase):
                f.write("{}\n".format(i))
