import sqlite3
import csv
import json
import pprint
import textwrap

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'
def init_db():
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
    cur = conn.cursor()
    # Drop tables
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)
    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)

    conn.commit()

    statement = '''
        CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT NOT NULL,
            'SpecificBeanBarName' TEXT NOT NULL,
            'REF' TEXT NOT NULL,
            'ReviewDate' TEXT NOT NULL,
            'CocoaPercent' REAL NOT NULL,
            'CompanyLocationId' INTEGER,
            'Rating' REAL NOT NULL,
            'BeanType' TEXT NOT NULL,
            'BroadBeanOriginId' INTEGER
        );
    '''
    cur.execute(statement)
    statement = '''
        CREATE TABLE 'Countries' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Alpha2' TEXT NOT NULL,
                'Alpha3' TEXT NOT NULL,
                'EnglishName' TEXT NOT NULL,
                'Region' TEXT NOT NULL,
                'Subregion' TEXT NOT NULL,
                'Population' INTEGER NOT NULL,
                'Area' REAL
        );
    '''
    cur.execute(statement)
    conn.commit()
    conn.close()

def insert_c():
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
    cur = conn.cursor()
    with open("countries.json", 'r') as f:
        c = json.loads(f.read())
        for i in c:
            insertion = (None, i['alpha2Code'], i['alpha3Code'], i['name'], i['region'], i['subregion'], i['population'], i['area'])
            statement = '''
                INSERT INTO Countries
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''
            cur.execute(statement, insertion)
    conn.commit()
    conn.close()

def insert_b():
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
    cur = conn.cursor()
    with open("flavors_of_cacao_cleaned.csv", 'r') as f:
        frows = csv.reader(f)
        first_row = 0
        for row in frows:
            first_row += 1
            if first_row == 1:
                continue
            try:
                cid = cur.execute("SELECT Id FROM Countries WHERE EnglishName = ?", (row[5],)).fetchone()[0]
            except:
                cid = None
            try:
                bid = cur.execute("SELECT Id FROM Countries WHERE EnglishName = ?", (row[8],)).fetchone()[0]
            except:
                bid = None
            insertion = (None, row[0], row[1], row[2], row[3], float(row[4][:-1])/100, cid, row[6], row[7], bid)
            statement = '''
                        INSERT INTO Bars
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
            cur.execute(statement, insertion)
    conn.commit()
    conn.close()

# Part 2: Implement logic to process user commands
def process_bar(bar11, bar12, rc, od, num):
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
    cur = conn.cursor()
    if bar11 is None and bar12 is None:
        statement = '''
            SELECT b.SpecificBeanBarName, b.Company, c1.EnglishName, b.Rating, b.CocoaPercent, c2.EnglishName
            FROM Bars AS b
            LEFT JOIN Countries as c1
            on b.CompanyLocationId = c1.Id
            LEFT JOIN Countries as c2
            on b.BroadBeanOriginId = c2.Id
            ORDER BY {} {}
            LIMIT {}
        '''
        result = cur.execute(statement.format(rc, od, num)).fetchall()
    else:
        statement = '''
            SELECT b.SpecificBeanBarName, b.Company, c1.EnglishName, b.Rating, b.CocoaPercent, c2.EnglishName
            FROM Bars AS b
            LEFT JOIN Countries as c1
            on b.CompanyLocationId = c1.Id
            LEFT JOIN Countries as c2
            on b.BroadBeanOriginId = c2.Id
            WHERE {} = \'{}\'
            ORDER BY {} {}
            LIMIT {}
        '''
        result = cur.execute(statement.format(bar11, bar12, rc, od, num)).fetchall()
    conn.close()
    return result

def process_com(com11, com12, od, num, agg):
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
    cur = conn.cursor()
    if com11 is None and com12 is None:
        statement = '''
            SELECT b.Company, c1.EnglishName, {}
            FROM Bars AS b
            LEFT JOIN Countries as c1
            on b.CompanyLocationId = c1.Id
            GROUP BY b.Company
            HAVING COUNT (*) > 4
            ORDER BY {} {}
            LIMIT {}
        '''
        result = cur.execute(statement.format(agg, agg, od, num)).fetchall()
    else:
        statement = '''
            SELECT b.Company, c1.EnglishName, {}
            FROM Bars AS b
            LEFT JOIN Countries as c1
            on b.CompanyLocationId = c1.Id
            WHERE {} = \'{}\'
            GROUP BY b.Company
            HAVING COUNT (*) > 4
            ORDER BY {} {}
            LIMIT {}
        '''
        result = cur.execute(statement.format(agg, com11, com12, agg, od, num)).fetchall()
    conn.close()
    return result

def process_cou(cou11, cou12, od, num, agg, ss):
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
    cur = conn.cursor()
    if cou11 is None and cou12 is None:
        statement = '''
            SELECT {}.EnglishName, {}.Region, {}
            FROM Bars AS b
            LEFT JOIN Countries as c1
            on b.CompanyLocationId = c1.Id
            LEFT JOIN Countries as c2
            on b.BroadBeanOriginId = c2.Id
            GROUP BY {}.EnglishName
            HAVING COUNT (*) > 4
            ORDER BY {} {}
            LIMIT {}
        '''
        result = cur.execute(statement.format(ss, ss, agg, ss, agg, od, num)).fetchall()
    else:
        statement = '''
            SELECT {}.EnglishName, {}.Region, {}
            FROM Bars AS b
            LEFT JOIN Countries as c1
            on b.CompanyLocationId = c1.Id
            LEFT JOIN Countries as c2
            on b.BroadBeanOriginId = c2.Id
            WHERE {}{} = \'{}\'
            GROUP BY {}.EnglishName
            HAVING COUNT (*) > 4
            ORDER BY {} {}
            LIMIT {}
        '''
        result = cur.execute(statement.format(ss, ss, agg, ss, cou11, cou12, ss, agg, od, num)).fetchall()
    conn.close()
    return result

def process_reg(od, num, agg, ss):
    try:
        conn = sqlite3.connect(DBNAME)
    except:
        print("fail to connect to database")
    cur = conn.cursor()
    statement = '''
        SELECT {}.Region, {}
        FROM Bars AS b
        LEFT JOIN Countries as c1
        on b.CompanyLocationId = c1.Id
        LEFT JOIN Countries as c2
        on b.BroadBeanOriginId = c2.Id
        WHERE {}.Region IS NOT NULL
        GROUP BY {}.Region
        HAVING COUNT (*) > 4
        ORDER BY {} {}
        LIMIT {}
        '''
    result = cur.execute(statement.format(ss, agg, ss, ss, agg, od, num)).fetchall()
    conn.close()
    return result

def process_command(command):
    bar11 = None
    bar12 = None
    com11 = None
    com12 = None
    cou11 = None
    cou12 = None
    rc = "Rating"
    num = '10'
    od = "DESC"
    agg = "AVG(b.Rating)"
    ss = "c1"
    result = None
    com_type = ""
    command_list = command.split()
    for i in range(0, len(command_list)):
        if command_list[i] == 'bars' or command_list[i] == 'companies' or command_list[i] == 'countries' or command_list[i] == 'regions':
            com_type = command_list[i]
        elif command_list[i] == 'ratings':
            rc = "Rating"
            agg = "AVG(b.Rating)"
        elif command_list[i] == 'cocoa':
            rc = "CocoaPercent"
            agg = "AVG(b.CocoaPercent)"
        elif command_list[i][0:3] == 'top':
            num = command_list[i][4:]
            od = "DESC"
        elif command_list[i][0:6] == 'bottom':
            num = command_list[i][7:]
            od = 'ASC'
        elif command_list[i][0:9] == "bars_sold":
            agg = "COUNT (*)"
        elif command_list[i][0:7] == "sellers":
            ss = "c1"
        elif command_list[i][0:7] == "sources":
            ss = "c2"
        elif command_list[i][0:11] == "sellcountry":
            bar11 = "c1.Alpha2"
            bar12 = command_list[i][12:]
        elif command_list[i][0:13] == "sourcecountry":
            bar11 = "c2.Alpha2"
            bar12 = command_list[i][14:]
        elif command_list[i][0:10] == "sellregion":
            bar11 = "c1.Region"
            bar12 = command_list[i][11:]
        elif command_list[i][0:12] == "sourceregion":
            bar11 = "c2.region"
            bar12 = command_list[i][13:]
        elif command_list[i][0:7] == "country":
            com11 = "c1.Alpha2"
            com12 = command_list[i][8:]
        elif command_list[i][0:6] == "region":
            com11 = "c1.Region"
            com12 = command_list[i][7:]
            cou11 = ".Region"
            cou12 = command_list[i][7:]
        else:
            print("Command not recognized: {}".format(command))
            return

    if com_type == 'bars':
        result = process_bar(bar11, bar12, rc, od, num)

    if com_type == 'companies':
        result = process_com(com11, com12, od, num, agg)

    if com_type == 'countries':
        result = process_cou(cou11, cou12, od, num, agg, ss)

    if com_type == 'regions':
        result = process_reg(od, num, agg, ss)
    return result


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue
        elif response == 'exit':
            print("bye")
            return
        else:
            result = process_command(response)
            if result is not None:
                for i in result:
                    for j in i:
                        if type(j) == str:
                            mat = "{:12}\t"
                        if type(j) == float:
                            mat = "{:5}\t"
                            if j > 1.0:
                                j = round(j, 1)
                            if j <= 1.0:
                                j = int(j*100)
                                j = str(j)+"%"
                        if j is None:
                            j = "Unkown"
                            mat = "{:12}\t"
                        j = str(j)
                        if len(j) > 12:
                            j = j[:12]
                            j += "..."
                        print(mat.format(j), end="")
                    print()
                print()


# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    init_db()
    insert_c()
    insert_b()
    # process_command("companies ratings top=8")
    interactive_prompt()
    # bars ratings
    # bars sellcountry=US cocoa bottom=5
    # companies region=Europe bars_sold
    # companies ratings top=8
