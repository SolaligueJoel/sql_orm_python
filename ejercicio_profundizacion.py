import csv
import requests 
import sqlalchemy 
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker




engine = sqlalchemy.create_engine("sqlite:///database.db")
base = declarative_base() 

class Producto(base):
        __tablename__ = 'database'
        id = Column(String,primary_key=True,autoincrement=False)
        site_id = Column(String)
        title = Column(String)
        price = Column(Integer)
        currency_id = Column(String)
        initial_quantity = Column(Integer)
        available_quantity = Column(Integer)
        sold_quantity = Column(Integer)

        def __repr__(self):
                return f"""PRODUCTO\nid:{self.id}, site_id:{self.site_id}, title:{self.title}, price:{self.price}
                currency_id:{self.currency_id}, initial_quantity:{self.initial_quantity}, available_quantity:{self.available_quantity}, 
                sold_quantity:{self.sold_quantity}"""

         
def create_schema():
        base.metadata.drop_all(engine)
        base.metadata.create_all(engine)

def fill():
        Session = sessionmaker(bind=engine)
        session = Session()
        with open('meli_technical_challenge_data.csv','r') as File:
                reader = list(csv.DictReader(File))
                #print(reader)
                for row in reader:
                        try:
                                id_producto = (row["site"]+row["id"])
                                url = 'https://api.mercadolibre.com/items?ids={}'.format(id_producto)
                                response = requests.get(url).json()
                                data = response[0]["body"]
                                #print(data)       
                                producto = Producto(id=data["id"] ,site_id=data["site_id"],title=data["title"],price=data["price"],
                                                currency_id=data["currency_id"],initial_quantity=data["initial_quantity"],available_quantity=data["available_quantity"],
                                                sold_quantity=data["sold_quantity"])
                                session.add(producto)
                                session.commit()        
                          
                        except:
                                pass                                                

def fetch(id1):
        Session = sessionmaker(bind=engine)
        session = Session()
        query = session.query(Producto).filter(Producto.id==id1).first()
        if query:
                print('\n------------------------------')
                print(f'Imprimiendo fila del ID:{id1}')
                print(query)
        else:
                print("None")                


        
def compras(list_id):
        count = 0
        for x in list_id:
                url = 'https://api.mercadolibre.com/items?ids={}'.format(x)
                data = requests.get(url).json()
                new_data = data[0]["body"]
                data_price =  new_data["price"]
                count += data_price
        print(('\n------------------------------'))        
        print ('La suma de su compra es de: $',count,'\n')

if __name__ == '__main__':     
        #Crear DB
        create_schema()
        
        #Completar la DB con el csv
        fill()
        
        #Buscar por id
        id1 = "MLA845041373"
        id2 = "MLA717159516"
        fetch(id1)
        
        #Lista IDs producto del cliente
        list_id = ["MLA845041373","MLA806113846","MLA849015677","MLA823408605","MLA617061280"]
        compras(list_id)
