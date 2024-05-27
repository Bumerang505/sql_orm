import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import *

driver_db = input('Введите драйвер подключения (например: postgresql) : ')
login_db = input('Введите логин : ')
password_db = input('Введите пароль : ')
hostname_db = input('Введите название сервера (например: localhost) : ')
port_db = input('Введите порт сервера (например: 5432) : ')
name_db = input('Введите название базы данных: ')

DSN = f'{driver_db}://{login_db}:{password_db}@{hostname_db}:{port_db}/{name_db}'

engine = sqlalchemy.create_engine(DSN)

Session = sessionmaker(bind=engine)
session = Session()

remove_tables(engine)
create_tables(engine)


def parsing_data_from_json(json_path):
    with open(json_path, encoding='utf-8') as f:
        models_items = json.load(f)
        for lst in models_items:
            if lst['model'] == 'publisher':
                name = lst['fields']['name']
                publisher_add = Publisher(name=name)
                session.add(publisher_add)
                session.commit()

            if lst['model'] == 'book':
                title = lst['fields']['title']
                id_publisher = lst['fields']['id_publisher']
                book_add = Book(title=title, id_publisher=id_publisher)
                session.add(book_add)
                session.commit()

            if lst['model'] == 'shop':
                name = lst['fields']['name']
                shop_add = Shop(name=name)
                session.add(shop_add)
                session.commit()

            if lst['model'] == 'stock':
                id_book = lst['fields']['id_book']
                id_shop = lst['fields']['id_shop']
                count = lst['fields']['count']
                stock_add = Stock(id_book=id_book, id_shop=id_shop, count=count)
                session.add(stock_add)
                session.commit()

            if lst['model'] == 'sale':
                price = lst['fields']['price']
                date_sale = lst['fields']['date_sale']
                id_stock = lst['fields']['id_stock']
                count = lst['fields']['count']
                sale_add = Sale(price=price, date_sale=date_sale, id_stock=id_stock, count=count)
                session.add(sale_add)
                session.commit()


def get_books_info(publisher_name_id):
    select = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).select_from(Shop). \
        join(Stock). \
        join(Book). \
        join(Publisher). \
        join(Sale)
    if publisher_name_id.isdigit():
        select_result = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).select_from(Shop). \
            join(Stock). \
            join(Book). \
            join(Publisher). \
            join(Sale). \
            filter(Publisher.id == publisher_name_id).all()
    else:
        select_result = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).select_from(Shop). \
            join(Stock). \
            join(Book). \
            join(Publisher). \
            join(Sale). \
            filter(Publisher.name == publisher_name_id).all()

    for Book.title, Shop.name, Sale.price, Sale.date_sale in select_result:
        print(f'{Book.title} | {Shop.name} | {Sale.price} | {Sale.date_sale}')


parsing_data_from_json('test_data.json')
if __name__ == '__main__':
    input_publisher = input('Введите имя или id издателя: ')
    get_books_info(input_publisher)
session.close()
