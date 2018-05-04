import sys
import os
import pathlib
import shutil
import json
from antlr4 import *
from sqlLexer import sqlLexer
from sqlParser import sqlParser
from sqlListener import sqlListener
from antlr4.error.ErrorListener import ErrorListener
bdActual = " "
bdne = "La base de datos que esta tratando de accesar no existe"
ingresePls = "Ingrese a una base de datos primero"
tiposValidos = ["INT", "FLOAT", "DATE", "CHAR", "int", "float", "date", "char"]

class plsListener(ParseTreeListener):

    def exitCreate_database_stmt(self, ctx:sqlParser.Create_database_stmtContext):
        if bdActual == " ":
            os.makedirs("Databases/" + ctx.database_name().getText())
            dict = {'tablas':[]}
            with open("Databases/"+ ctx.database_name().getText() +"/schema.json", "w+") as outfile:
                json.dump(dict, outfile)
        else:
            os.makedirs("../" + ctx.database_name().getText())
            dict = {'tablas':[]}
            with open("../"+ ctx.database_name().getText() +"/schema.json", "w+") as outfile:
                json.dump(dict, outfile)

    def exitShow_databases_stmt(self, ctx:sqlParser.Show_databases_stmtContext):
        if bdActual != " ":
            for x in os.walk("../"):
                print(x[0].replace("../", ""))
        else:
            for x in os.walk("Databases/"):
                print(x[0].replace("Databases/", ""))

    def exitDrop_database_stmt(self, ctx:sqlParser.Drop_database_stmtContext):
        try:
            if bdActual != " ":
                shutil.rmtree("../../Databases/"+ctx.database_name().getText())
            else:
                shutil.rmtree("Databases/"+ctx.database_name().getText())
        except Exception as e:
            print (bdne)

    def exitShow_columns_stmt(self, ctx:sqlParser.Show_columns_stmtContext):
        try:
            if bdActual != " ":
                data = json.load(open(ctx.table_name().getText()+"/schema.json"))
                for x in data['campos']:
                    const = ""
                    for y in data ['constraints']:
                        if y['columna'] == x['nombre']:
                            const = const + ", " + y['constraint']
                    print("     *" + x['nombre'] + " de tipo " + x['tipo'] + " " + const)
                print(ctx.table_name().getText())
            else:
                print(ingresePls)
        except Exception as e:
            print (e)
            print (bdne)


    def exitCreate_table_stmt(self, ctx:sqlParser.Create_table_stmtContext):
        if(bdActual!=" "):
            dict = {'nombre':ctx.table_name().getText(), 'campos':[], 'constraints':[] }
            data = {}
            for x in range(len(ctx.column_def())):
                #f.write(ctx.column_def()[x].column_name().getText())
                #f.write(":"+ctx.column_def()[x].type_name().getText()+"\n")
                if ctx.column_def()[x].type_name().getText().split("(")[0] in tiposValidos:
                    dict['campos'].append({'nombre':ctx.column_def()[x].column_name().getText(), 'tipo':ctx.column_def()[x].type_name().getText()})
                    data[ctx.column_def()[x].column_name().getText()] = []
                else:
                    print("El tipo de dato de " + ctx.column_def()[x].column_name().getText() + " no es valido")
                    return
            for x in range(len(ctx.table_constraint())):
                for y in range(len(ctx.table_constraint()[x].column_name())):
                    #print(ctx.table_constraint()[x].column_name()[y].getText())
                    if ctx.table_constraint()[x].K_PRIMARY()!=None:
                        print("PRIMARY")
                        #f.write("PRIMARY_KEY:"+ctx.table_constraint()[x].column_name()[y].getText()+"\n")
                        dict['constraints'].append({'columna':ctx.table_constraint()[x].column_name()[y].getText(), 'constraint':'PRIMARY KEY'})
                    if ctx.table_constraint()[x].K_FOREIGN()!=None:
                        #print("FOREIGN")
                        #f.write("FOREIGN_KEY:"+ctx.table_constraint()[x].column_name()[y].getText()+" REFERENCES:"+ctx.table_constraint()[x].foreign_key_clause().foreign_table().getText()+"\n")
                        if(ctx.table_constraint()[x].foreign_key_clause().K_REFERENCES()!=None and len(ctx.table_constraint()[x].foreign_key_clause().foreign_table().getText().replace(" ",""))!=0):
                            data= json.load(open("schema.json"))
                            for tbl in data['tablas']:
                                #print (y)
                                if (tbl==ctx.table_constraint()[x].foreign_key_clause().foreign_table().getText()):
                                    print("yas?")
                                    dict['constraints'].append({'columna':ctx.table_constraint()[x].column_name()[y].getText(), 'constraint':'REFERENCES'+ ctx.table_constraint()[x].foreign_key_clause().foreign_table().getText()})
                                else:
                                    print(tbl)
                    if ctx.table_constraint()[x].K_UNIQUE()!=None:
                        print("UNIQUE")
                        #f.write("UNIQUE:"+ctx.table_constraint()[x].column_name()[y].getText()+"\n")
                        dict['constraints'].append({'columna':ctx.table_constraint()[x].column_name()[y].getText(), 'constraint':'UNIQUE'})
                    if ctx.table_constraint()[x].K_CHECK()!=None:
                        print("CHECK")
                        #f.write("CHECK:"+ctx.table_constraint()[x].column_name()[y].getText()+"\n")
                        dict['constraints'].append({'columna':ctx.table_constraint()[x].column_name()[y].getText(), 'constraint':'CHECK'})
            #f.close()
            print (dict)
            #Agregar tabla al schema de la base de datos
            base = json.load(open("schema.json"))
            base['tablas'].append(ctx.table_name().getText())
            with open("schema.json", "w+") as outfile:
                json.dump(base, outfile)


            os.makedirs(ctx.table_name().getText())
            #f=open(ctx.table_name().getText()+"/"+"schema.txt", "w+")

            with open(ctx.table_name().getText()+"/data.json", "w+") as outfile:
                json.dump(data, outfile)
            with open(ctx.table_name().getText()+"/"+"schema.json", "w+") as outfile:
                json.dump(dict, outfile)
        else:
            print(ingresePls)


    def exitShow_tables_stmt(self, ctx:sqlParser.Show_tables_stmtContext):
        if bdActual != " ":
            for x in os.walk("."):
                print(x[0].replace("./", ""))
        else:
            print (ingresePls)

    def exitInsert_stmt(self, ctx:sqlParser.Insert_stmtContext):
        if bdActual != " ":
            data = json.load(open(ctx.table_name().getText() + "/data.json"))
            try:
                td = []
                campos = []
                tabla = json.load(open(ctx.table_name().getText() + "/schema.json"))
                for campo in tabla['campos']:
                    campos.append(campo['nombre'])
                    td.append(campo['tipo'])
            except Exception as e:
                print(bdne)
                return

            #metio los argumentos necesarios?
            if len(ctx.expr()) == len(ctx.column_name()):
                pass 
            else:
                print("El numero de columnas ingresadas y el de datos ingresados no concuerda")
                return
            #recorrer lo que ingreso el usuario
            for x in range(len(ctx.column_name())):
                if ctx.column_name()[x].getText() in campos:
                    data[ctx.column_name()[x].getText()].append(ctx.expr()[x].getText())
                else:
                    print("La columna " + x.getText() + " no existe")
        else:
            print (ingresePls)
        with open(ctx.table_name().getText()+"/"+"data.json", "w+") as outfile:
            json.dump(data, outfile)


    def exitUpdate_stmt(self, ctx:sqlParser.Update_stmtContext):
    	if bdActual != " " :
    		data = json.load(open(ctx.table_name().getText() + "/data.json"))
    		columnLista = ctx.column_name()
    		exprList = ctx.expr()
    		print(exprList[1])
    		for i in range(len(exprList)):
    			if(i == len(exprList)-1):
    				where = exprList[i].getText()
    				where1 = where.split("=")
    		valorcito = where1[-1]
    		valorsote = where1[0]
    		indexerino = data[valorsote].index(valorcito)
    		for i in range(len(columnLista)):
    			data[ctx.column_name()[i].getText()].pop(indexerino)
    			data[ctx.column_name()[i].getText()].insert(indexerino,ctx.expr()[i].getText())
    	with open(ctx.table_name().getText()+"/"+"data.json", "w+") as outfile:
            json.dump(data, outfile)
  



    def exitAlter_table_stmt(self, ctx:sqlParser.Alter_table_stmtContext):
        if bdActual != " ":
            data= json.load(open("schema.json"))
            for x in data['tablas']:
                if (x== ctx.table_name().getText()):
                    #ADD/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    try:
                        ctx.alter_table_specific_stmt().K_ADD()
                        ctx.alter_table_specific_stmt().K_COLUMN()
                        data2 = json.load(open(ctx.table_name().getText()+"/schema.json"))
                        for y in data2['campos']:
                            if (y['nombre']==ctx.alter_table_specific_stmt().column_def().column_name().getText()):
                                print("La tabla ya existe")
                                val = False
                            else:
                                if(ctx.alter_table_specific_stmt().column_def().type_name().getText() not in tiposValidos):
                                    print ("El tipo no es valido")
                                    val= False
                                else:
                                    val= True
                        if (val):
                            print ("Add Valid")
                            data2['campos'].append({'nombre':ctx.alter_table_specific_stmt().column_def().column_name().getText(), 'tipo':ctx.alter_table_specific_stmt().column_def().type_name().getText()})
                            with open(ctx.table_name().getText()+"/schema.json", "w+") as outfile:
                                json.dump(data2, outfile)
                            data3 = json.load(open(ctx.table_name().getText()+"/data.json"))
                            data3[ctx.alter_table_specific_stmt().column_def().column_name().getText()]=[]
                            for z in range(len(data3[data2['campos'][0]['nombre']])):
                                data3[ctx.alter_table_specific_stmt().column_def().column_name().getText()].append(None)
                            with open(ctx.table_name().getText()+"/data.json", "w+") as outfile:
                                json.dump(data3, outfile)
                            break
                        else:
                            print("Add not valid")
                    except Exception as e:
                        #print(e)
                        print("No es ADD")
                    #DROP///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    try:
                        ctx.alter_table_specific_stmt().K_DROP()
                        ctx.alter_table_specific_stmt().K_COLUMN()
                        data2 = json.load(open(ctx.table_name().getText()+"/schema.json"))
                        for y in data2['campos']:
                            if (y['nombre']== ctx.alter_table_specific_stmt().column_name().getText() and y['constraint']=="PRIMARY KEY"):
                                ver=False
                                print("Es una PRIMARY KEY")
                            else:
                                print("Valid DROP")
                                ver= True
                                break
                        if(ver):
                            print("nani?")
                            i = data2['campos'].index(ctx.alter_table_specific_stmt().column_name().getText())
                            del data2['campos'][i]
                            with open("schema.json", "w+") as outfile:
                                json.dump(data2, outfile)
                            print("nani2?")
                            data3 = json.load(open(ctx.table_name().getText()+"/data.json"))
                            del data3[ctx.alter_table_specific_stmt().column_name().getText()]
                            with open("schema.json", "w+") as outfile:
                                json.dump(data3, outfile)
                            break
                        else:
                            print("La tabla no existe")
                    except Exception as e:
                        print (e)
                        #print("No es un DROP COLUMN")
                    #DROP CONSTRINT /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    try:
                        ctx.alter_table_specific_stmt().K_DROP()
                        break
                    except Exception as e:
                        #print(e);
                        print("no es DROP CONSTRAINT")

                else:
                    print("halp")
                    
        else:
            print (ingresePls)

    def exitAlter_database_stmt(self, ctx:sqlParser.Alter_database_stmtContext):
        global bdActual
        try:
            if bdActual == " ":
                os.rename("Databases/" + ctx.database_name().getText(), "Databases/" + ctx.new_database_name().getText())
            else:
                os.chdir(os.path.dirname(os.getcwd()))
                os.rename(ctx.database_name().getText(), ctx.new_database_name().getText())
        except Exception as e:
            print (bdne)

    def exitUse_database_stmt(self, ctx:sqlParser.Use_database_stmtContext):
        global bdActual
        try:
            if bdActual == " ":
                os.chdir("Databases/" + ctx.database_name().getText())
            else:
                os.chdir(os.path.dirname(os.getcwd()))
                os.chdir(ctx.database_name().getText())
            bdActual = ctx.database_name().getText()
        except Exception as e:
            print (bdne)


    def exitDrop_table_stmt(self, ctx:sqlParser.Drop_table_stmtContext):
        if bdActual != " ":
            try:
                shutil.rmtree(ctx.table_name().getText())
                base = json.load(open("schema.json"))
                x = base['tablas'].index(ctx.table_name().getText())
                del base['tablas'][x]
                with open("schema.json", "w+") as outfile:
                    json.dump(base, outfile)
            except Exception as e:
                print("Esa tabla no existe")
        else:
            print(ingresePls)



class ParserException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParserExceptionErrorListener(ErrorListener):
    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        raise ParserException("line " + str(line) + ":" + str(column) + " " + msg)

def parse(text):
    lexer = sqlLexer(InputStream(text))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParserExceptionErrorListener())

    stream = CommonTokenStream(lexer)

    parser = sqlParser(stream)
    parser.removeErrorListeners()
    parser.addErrorListener(ParserExceptionErrorListener())

    # Este es el nombre de la produccion inicial de la gramatica definida en sql.g4
    tree = parser.parse()

    testListener = plsListener()
    walker  = ParseTreeWalker()
    walker.walk(testListener, tree)

'''
Uso: python cli.py

Las construcciones validas para esta gramatica son todas aquellas
'''
def main(argv):
    while True:
        try:
            text = input(" " + bdActual + " > ")

            if (text == 'exit'):
                sys.exit()

            parse(text);

        except ParserException as e:
            print("Got a parser exception:", e.value)

        except EOFError as e:
            print("Bye")
            sys.exit()

        except Exception as e:
            print("Got exception: ", e)

if __name__ == '__main__':
    main(sys.argv)
