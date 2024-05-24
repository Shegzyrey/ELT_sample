
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask.cli import FlaskGroup
from faker import Faker
import click


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:Root#123@oltp-db:5432/oltp_postgres'
db = SQLAlchemy(app)


cli = FlaskGroup(app)

class Customer(db.Model):
    customer_id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(80), nullable=False)
    last_name = db.Column(db.String(80), nullable=False)
    email = db.Column(db.String(120), nullable=False)
    create_date = db.Column(db.Date, nullable=False)
    last_update = db.Column(db.DateTime, nullable=False)

fake = Faker()

@cli.command('fill_data')
@click.option('--count', default=1, help='Number of records to add')
def fill_data(count):
    for _ in range(count):
        customer = Customer(
            customer_id=fake.random_number(10),
            first_name=fake.unique.first_name(),
            last_name=fake.unique.last_name(),
            email=fake.email(),
            create_date=fake.date(),
            last_update=fake.date_time_this_decade()
        )
        db.session.add(customer)
    db.session.commit()
    click.echo(f"{count} data record(s) added successfully!")

if __name__ == '__main__':
    cli()