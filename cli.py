import sys
import os
import pathlib
import shutil
from antlr4 import *
from sqlLexer import sqlLexer
from sqlParser import sqlParser
from sqlListener import sqlListener
from antlr4.error.ErrorListener import ErrorListener
bdActual = " "
bdne = "La base de datos que esta tratando de accesar no existe"
ingresePls = "Ingrese a una base de datos primero"

class plsListener(ParseTreeListener):

    def exitCreate_database_stmt(self, ctx:sqlParser.Create_database_stmtContext):
        os.makedirs("Databases/" + ctx.database_name().getText())


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


    def exitCreate_table_stmt(self, ctx:sqlParser.Create_table_stmtContext):
        os.makedirs(ctx.table_name().getText())

    def exitShow_tables_stmt(self, ctx:sqlParser.Show_tables_stmtContext):
        if bdActual != " ":
            for x in os.walk("."):
                print(x[0].replace("./", ""))
        else:
            print (ingresePls)

    def exitAlter_table_stmt(self, ctx:sqlParser.Alter_table_stmtContext):
        if bdActual != " ":
            os.rename(ctx.table_name().getText(),ctx.new_table_name().getText())
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
        shutil.rmtree(ctx.table_name().getText())



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
            text = input(bdActual + "> ")

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
