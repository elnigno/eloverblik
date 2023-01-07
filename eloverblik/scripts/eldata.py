import click
from datetime import datetime
from eloverblik.eloverblik import DatabaseBuilder


@click.group()
def eloverblik():
    pass


@click.command(help='First time setup: Downloads all data since 2019')
def initdb():
    click.echo('Initializing the database...')
    start = datetime.now()
    db = DatabaseBuilder()
    db.build_dataset()
    click.echo(f"DB initialized in {str(datetime.now() - start)}")


@click.command(help='Updates the database with recent data')
def update():
    click.echo('Updating the database...')
    start = datetime.now()
    db = DatabaseBuilder()
    db.update_dataset()
    click.echo(f"DB updated in {str(datetime.now() - start)}")


eloverblik.add_command(initdb)
eloverblik.add_command(update)
