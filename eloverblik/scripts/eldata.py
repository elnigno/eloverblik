import click
from datetime import datetime
from streamlit.web.cli import _main_run
from eloverblik.eloverblik import DatabaseBuilder
from eloverblik.tools import basepath, datapath


@click.group()
def eloverblik():
    pass


@click.command(help='First time setup: Constructs database')
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


@click.command(help='Starts the dashboard, eventually constructing the database')
def dashboard():
    if datapath.exists() is False:
        click.echo('Initializing the database...')
        start = datetime.now()
        db = DatabaseBuilder()
        db.build_dataset()
        click.echo(f"DB initialized in {str(datetime.now() - start)}")
    args=[]
    _main_run((basepath / 'streamlit_app.py').as_posix() , args)


eloverblik.add_command(initdb)
eloverblik.add_command(update)
eloverblik.add_command(dashboard)
