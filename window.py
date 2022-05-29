from tkinter import *
from clickhouse_driver import Client

import settings

def connect_to_db(client):
    client.execute("CREATE DATABASE IF NOT EXISTS topics")

def start(data, topic):  

    def save_configuration(keys, field_names, values, topic):
        i = 0

        config = open(f"{topic}.txt", "w+") #файл конфигурации

        request = f'CREATE TABLE IF NOT EXISTS topics.{topic}('
        req_values = f'INSERT INTO topics.{topic} VALUES ('

        for name in field_names:
            config.write(f'{keys[i]}:{name.get()}\n')

            request += f'{name.get()} '
            if (type(values[i]) == int):
                request += 'Int32,'
                req_values += f'{values[i]}, '
            else:
                request += 'String,'
                req_values += f"'{values[i]}', "
            i += 1

        config.close()

        request = request[0:len(request)-1] #убираем завершающую запятую
        req_values = req_values[0:len(req_values)-2]

        req_values += ');'
        request += f') ENGINE = Memory'

        client = Client('localhost')
        res = client.execute(request) #создаем таблицу если ее нет
        res = client.execute(req_values) #вставляем данные


    client = Client('localhost')
    connect_to_db(client)

    window = Tk()
    window.title("Configuration DB")
    window.geometry('500x3000')

    keys = list(data.keys())
    values = list(data.values())

    entry_vars = []
    last_row = 0
    
    #CREATE INTERFACE
    for i in range(0, len(keys)):
        lbl = Label(text=keys[i])
        ent = Entry(window)
        title = Label(text="Ключ")
        title2 = Label(text="Значение")
        title3 = Label(text="Имя поля в БД")
        #generate variables for entries
        var_i = StringVar()
        entry_vars.append(var_i)
        
        #set vars for entries
        field_name_field = Entry(window, textvariable=entry_vars[i])

        ent.insert(0, values[i])

        title.grid(row=0, column=0)
        title2.grid(row=0, column=1)
        title3.grid(row=0, column=2)

        lbl.grid(row=i+5, column=0)
        ent.grid(row=i+5, column=1)
        field_name_field.grid(row=i+5, column=2)

        last_row=i+6
    
    b1 = Button(text="Сохранить конфигурацию", height=2)
    b1.config(command=lambda: save_configuration(keys, entry_vars, values, topic))
    b1.grid(row=last_row, column=1, columnspan = 2)

    window.mainloop()
    return 1