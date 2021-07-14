import click

from graphpipeline.datasource import BaseDataSource, VersionedRemoteDataSource
from biomedgraph.datasources import ALL_DATASOURCES


@click.group()
def cli():
    pass


@click.group()
def datasources():
    pass


@click.command()
def list():
    click.echo('Known datasources')
    click.echo('===========================')
    datasources_found = []
    for name, x in ALL_DATASOURCES.items():
        if isinstance(x, type) and issubclass(x, BaseDataSource) and x.__name__ != BaseDataSource.__name__ and x.__name__ != 'Dummy':
            datasources_found.append(x)
    for d in datasources_found:
        click.echo(d.__name__)


@click.command()
@click.option('--target', help='Root directory for downloads.')
@click.option('--version', default='latest', help='Version to download, default is latest.')
@click.option('--datasource', help='Name of the datasource.')
@click.option('--all', is_flag=True, help='Name of the datasource.')
def download(target, version, datasource, all):

    if all:
        for name, ds_class in ALL_DATASOURCES.items():
            click.echo(f'Download {name}')

            ds = ds_class(target)

            if isinstance(ds, VersionedRemoteDataSource):
                if version == 'latest':
                    ds.download(ds.latest_remote_version())
            else:
                ds.download()

    else:
        click.echo(f'Download {datasource}')

        ds_class = ALL_DATASOURCES[datasource]
        ds = ds_class(target)

        if isinstance(ds, VersionedRemoteDataSource):
            if version == 'latest':
                ds.download(ds.latest_remote_version())
        else:
            ds.download()


cli.add_command(datasources)
datasources.add_command(list)
datasources.add_command(download)

