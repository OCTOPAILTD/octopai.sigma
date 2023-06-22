# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import xmltodict
import os
import  pandas as pd
import json
from tabulate import tabulate
import requests
import json

class ReadFiles:
    def Getfiles(self, folder_path,fileTypev,elements=""):

        json_list=[]
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.json'):
                    try:
                        file_path = os.path.join(root, file)
                        fileType=file_path.split('\\')[3]

                        if fileType == fileTypev:
                            # Case 'a' logic
                            with open(file_path, 'r', encoding='utf-8') as file:
                                # Read the contents
                                file_contents = file.read()
                                json_data = json.loads(file_contents)
                                if fileType == "workbooks_schema" and elements=="":
                                    data=json_data["sheets"]
                                    output=[]
                                    try:
                                        for sheet in data:
                                            try:
                                                if "config" in  data[sheet]:
                                                    if "text" in  data[sheet]["config"]["title"]:
                                                     sheetName=data[sheet]["config"]["title"]["text"]
                                                    else:
                                                        sheetName=""
                                                else:
                                                    sheetName = ""


                                            except Exception as e:
                                                sheetName=""
                                                print(e)
                                            for column_id, column_data in data[sheet]["columns"].items():
                                                try:
                                                    columnName=""
                                                    if "format" in column_data["formula"]:

                                                        if "path" in column_data["formula"]:
                                                            columnName = column_data["formula"]["path"][1]
                                                        elif "another_key" in column_data["formula"]:
                                                            columnName = column_data["formula"]["another_key"]
                                                        elif "yet_another_key" in column_data["formula"]:
                                                            columnName = column_data["formula"]["yet_another_key"]
                                                        else:
                                                            columnName=""
                                                    dateType=""
                                                    if "dateType" in column_data["formula"]:
                                                        dateType=column_data["format"]["type"]




                                                        try:
                                                            output.append({
                                                            "workbookID":json_data["workbookId"],
                                                            "sheetID": sheet,
                                                            "sheetName":sheetName,
                                                            "columnID": column_id,
                                                            "type": column_data["type"],
                                                            "formulaType": column_data["formula"]["type"],
                                                            "path":  column_data["formula"]["path"][0] if "path" in   column_data["formula"] else "",
                                                            "columnName": columnName,
                                                            "DataType": dateType,
                                                            "format": ""
                                                            })
                                                        except Exception as e:
                                                            print(e)
                                                    elif  "op" in column_data["formula"]:
                                                        try:
                                                            output.append({
                                                                "workbookID": json_data["workbookId"],
                                                                "sheetID": sheet,
                                                                "sheetName": sheetName,
                                                                "columnID": column_id,
                                                                "type": column_data["type"],
                                                                "SourceColumns":column_data["formula"]["args"] if "args" in column_data["formula"] \
                                                                                                                    else [column_data["formula"]["x"],column_data["formula"]["y"]]
                                                                ,
                                                                "TargetColumnName": column_data["name"] if "name" in column_data else "",

                                                                "DataType":  column_data["format"]["type"] if "format" in column_data else "" ,
                                                                "format":    column_data["format"]["format"] if "format" in column_data else "" ,

                                                            })
                                                        except Exception as e:
                                                            print(e)

                                                except Exception as e:
                                                    print(e)

                                    # Convert to JSON string
                                        result = json.dumps(output)

                                        # Display the transformed JSON
                                        print(result)
                                        json_list.append(output)
                                    except Exception as e:
                                        print(e)
                                elif elements=="elements":

                                    for element in json_data["elements"].values():
                                        try:
                                            new_dict = {}
                                            if "viz" in element:
                                                new_dict["id"] = element["id"]
                                                new_dict["workbookId"] = json_data["workbookId"]
                                                new_dict["sheetId"] = element["viz"]["sheetId"]
                                                new_dict["type"] = element["type"]
                                                new_dict["htype"] = element["height"]["type"]
                                                json_list.append(new_dict)

                                        except Exception as e:
                                         print(e)


                                else:
                                    json_list.append(json_data)


                    except Exception as e:
                        print(e)

        listofdf=[]
        for entity in json_list:
            try:
                if fileTypev =="workbooks_queries":
                    df = pd.DataFrame(entity["entries"])
                elif fileTypev == "workbooks_schema" and elements=="":
                        df = pd.DataFrame(entity)


                else:
                    df = pd.DataFrame(entity, index=[0])


                listofdf.append(df)

            except Exception as e:
             print(e)
        return  pd.concat(listofdf)


cl=ReadFiles()

class Program:
    def GetDataFrames(self,path):

        listTypes=['connections','files','workbooks','workbooks_queries','workbooks_schema','workspaces']
        # for type in listTypes:
        #     print(f'Start {type}...')
        try:
            type="connections"
            dfConn= cl.Getfiles(path, type)
            type = "files"
            dfFiles = cl.Getfiles(path, type)
            type = "workbooks"
            dfWorkBook = cl.Getfiles(path, type)
            type = "workbooks_queries"
            dfWorkbooks_queries = cl.Getfiles(path, type)
            type = "workbooks_schema"
            dfWorkbooks_schema = cl.Getfiles(path, type)


            dfElements = cl.Getfiles(path, type,"elements")

            type = "workspaces"
            dfWorkspaces = cl.Getfiles(path, type)

            try:
                self.MakeJoinsBetweenDF(dfConn,dfFiles,dfWorkBook,dfWorkbooks_queries,dfWorkbooks_schema,dfWorkspaces,dfElements)
            except Exception as e:
                print(e)


        except Exception as e:
            print(e)

        print(f'Finish {type}')

    def MakeJoinsBetweenDF(self,dfConn,dfFiles,dfWorkBook,dfWorkbooks_queries,dfWorkbooks_schema,dfWorkspaces,dfElements):
        print("Connections:")
        # table = tabulate(dfConn, headers='keys', tablefmt='pipe')
        # print(table)
        print("Files:")        #
        # table = tabulate(dfFiles, headers='keys', tablefmt='pipe')
        # print(table)

        print("Workbooks:")
        # table = tabulate(dfWorkBook, headers='keys', tablefmt='pipe')
        # print(table)
        #
        print("Workbooks Queries:")
        # table = tabulate(dfWorkbooks_queries, headers='keys', tablefmt='pipe')
        # print(table)
        #
        print("Workbook Schema:")
        # table = tabulate(dfWorkbooks_schema, headers='keys', tablefmt='pipe')
        # print(table)
        #
        print("elements:")
        # table = tabulate(dfElements, headers='keys', tablefmt='pipe')
        # print(table)
        #
        #
        print("Workspace:")
        # table = tabulate(dfWorkspaces, headers='keys', tablefmt='pipe')
        # print(table)

        try:
            dfWbSchemaDist = dfWorkbooks_schema[['workbookID', 'sheetID','sheetName']].drop_duplicates().reset_index(drop=True)

            merged_df=self.JoinWorkbookName(dfWbSchemaDist,dfWorkBook)
            merged_df=self.JoinSheetIdAndElementID(merged_df,dfElements)
            merged_df=self.JoinElemetsAndSQL(merged_df,dfWorkbooks_queries)


            df=self.ParseAllQueries(merged_df)


            df["SourceDataType"]=""
            df["TargetDataType"] = ""

            condIsFinal = df['TargetLayerName'].str.contains(r'RS-\d+')
            condIsFinalSrc = df['SourceLayerName'].str.contains(r'RS-\d+')
            df.loc[condIsFinal, 'TargetLayerName'] =  df.loc[condIsFinal, 'ReportName']
            df.loc[condIsFinalSrc, 'SourceLayerName'] = df.loc[condIsFinalSrc, 'ReportName']

            df.loc[condIsFinal, 'TargetSQL'] = df.loc[condIsFinal, 'TargetSQL']
            df.loc[~condIsFinal, 'TargetSQL'] = ""

            df.loc[condIsFinal, 'TargetSQL'] = df.loc[condIsFinal, 'TargetSQL']
            df.loc[~condIsFinal, 'TargetSQL'] = ""
            df["SourceSQL"]=""

            # df['SourceObjectType'] = df.apply(
            #     lambda row: 'PresentationTable' if row['SourceObjectType'] in ['select_list','with_cte'] else  row['SourceObjectType'] ,
            #     axis=1)









            # dfDist=df[["ModelName","ReportPath","SourceLayerName","TargetLayerName"]].drop_duplicates()
            #
            # dfDistCopy=dfDist.copy()
            # try:
            #     dfIn=dfDist.merge(dfDistCopy, left_on=['ModelName','ReportPath','TargetLayerName'], right_on=['ModelName','ReportPath','SourceLayerName'],how='left')
            # except Exception as e:
            #     print(e)
            #

            df["SourceProvider"]="SNOWFLAKE"
            df["TargetProvider"] = ""
            df["SourceServer"]=''
            df["TargetServer"] = ''
            df["SourcePrecision"] = ''
            df["SourceScale"] = ''
            df["TargetPrecision"] = ''
            df["TargetScale"] = ''
            df["LinkType"] = ''
            df["LinkDescription"] = ''
            df["Expression"] = ''

            df=df[['ModelName',
                   'ReportPath',
                   'ReportName',
                   'SourceProvider',
                   'SourceLayerName',
                   'SourceServer',
                   'SourceSchema',
                   'SourceDB',
                   'SourceTable',
                   'SourceColumn',
                   'SourceDataType',
                   'SourceObjectType',
                   'SourcePrecision',
                   'SourceScale',
                   'TargetServer',
                   'TargetLayerName',
                   'TargetSchema',
                   'TargetDB',
                   'TargetTable',
                   'TargetColumn',
                   'TargetDataType',
                   'TargetPrecision',
                   'TargetScale',
                   'TargetSQL',
                   'LinkType',
                   'LinkDescription',
                   'Expression'
                   ]]
            # filtered_df = df[df['ReportName'] == 'Data Source - Key Metric Visualizations']
           # print(tabulate(filtered_df, headers='keys', tablefmt='pipe'))

            df=df.rename(columns={ 'ModelName':"Model Name",
                               'ReportPath':'Report Path',
                               'ReportName': "Report Name",
                               'SourceProvider': 'Source Provider',
                               'SourceLayerName': 'Source Layer Name',
                               'SourceServer': 'Source Server',
                               'SourceSchema': 'Source Schema',
                               'SourceDB':'Source Db',
                               'SourceTable':'Source Table',
                               'SourceColumn':'Source Column',
                               'SourceDataType':'Source Data Type',
                               'SourceObjectType': 'Source Object Type',
                               'SourcePrecision': 'Source Precision',
                               'SourceScale': 'Source Scale' ,
                               'TargetServer':'Target Server',
                               'TargetLayerName':'Target Layer Name' ,
                               'TargetSchema': 'Target Schema',
                               'TargetDB': 'Target Db',
                               'TargetTable': 'Target Table',
                               'TargetColumn': 'Target Column',
                               'TargetDataType': 'Target Data Type',
                               'TargetPrecision' : 'Target Precision',
                               'TargetScale': 'Target Scale',
                               'LinkDescription':'Link Description',
                               'LinkType': 'Link Type',
                               'Expression':'Expression'})










            try:
                df.to_csv("E:\\temp\\generic_objects.csv", index=False)
            except Exception as e:
                print(e)







        except Exception as e:
         print(e)

    def JoinElements(self,merged_df,dfElements):

        try:
            merged_df = merged_df.merge(dfElements, left_on='sheetID', right_on='sheetId')[
                ['workbookId', 'name', 'path', 'sheetID', 'sheetName']]

            return merged_df


        except Exception as e:
            print(e)

    def ParseAllQueries(self,df):
        listOfdf=[]

        for index, row in df.iterrows():

            SqlText = row['sql']
            elementid=row['elementID']
            workBookName=row["WorkBookName"]
            try:
                workBookPath = row["WorkBookPath"]
                sheetName = row["sheetName"]
                sheetID = row["sheetID"]
                print(f"Start parsing query number {index}: ")
                if index==14:
                    print("break")

                dictResult=self.GetParsedResultPerQuery(SqlText,'dbvsnowflake')
                print(f"End parsing query")


            except Exception as e:
                print(e)

            try:


                print('dfTables')
                dfTables = self.CreateTablesdf(dictResult)


                print('ResultSet')
                dfRsultSet=self.CreateResultdf(dictResult)


                print('Relationship')


                dfRelationShips=self.CreateRelationshipdf(dictResult)

                try:
                    merged_df = dfRelationShips.merge(dfTables, left_on=['sourceId','sourceParentID'], right_on=['ColumnID','TableID'], how='left')[
                        ['sourceId','sourceColumn','TableDB','TableType', 'TableName','TableSchema','targetId','targetColumn','targetParentName','sourceParentName',
                         'targetParentID',   'sourceParentID']]

                    merged_df = merged_df.rename(columns={'TableDB':'SourceDB','TableSchema':'SourceSchema',
                                                          'targetParentName':'TargetLayerName',
                                                          'sourceParentName':'SourceLayerName',
                                                          'TableName':'SourceTable',
                                                          'TableType':'SourceObjectType',
                                                          'targetColumn':'TargetColumn',
                                                          'sourceColumn':'SourceColumn'
                                                          })[['SourceDB','SourceSchema','SourceTable',
                                                              'SourceObjectType',
                                                              'SourceLayerName',
                                                              'SourceColumn',
                                                              'TargetLayerName',
                                                              'TargetColumn',
                                                              'targetParentID',
                                                              'targetId',
                                                              'sourceParentID',
                                                              'sourceId'
                                                              ]]

                    merged_df = merged_df.merge(dfRsultSet, left_on=['targetParentID', 'targetId'],
                                                      right_on=['RsID', 'ColumnID'], how='left').\
                                rename(columns={'RsType':'TargetObjectType'})

                    merged_df = merged_df.merge(dfRsultSet, left_on=['sourceParentID', 'sourceId'],
                                                right_on=['RsID', 'ColumnID'], how='left')

                    merged_df['SourceObjectType']=merged_df.apply(
                        lambda row: row['SourceObjectType'] if row['SourceObjectType'] == 'table' else 'PresentationTable',
                        axis=1)

                    merged_df['SourceTable']=merged_df.apply(
                        lambda row: row['SourceTable'].split('.')[-1]   if row['SourceObjectType']=='table' else '',
                        axis=1)

                    merged_df['SourceDB'] = merged_df.apply(
                        lambda row: row['SourceDB'] if row['SourceObjectType'] == 'table' else '',
                        axis=1)

                    merged_df['SourceSchema'] = merged_df.apply(
                        lambda row: row['SourceDB'] if row['SourceObjectType'] == 'table' else '',
                        axis=1)



                    merged_df['SourceLayerName'] =merged_df.apply(
                        lambda row: row['SourceTable'].split('.')[-1]   if row['SourceObjectType']=='table'
                        else row['SourceLayerName'] if row['SourceObjectType'] in [ 'select_list','with_cte']

                        else row['SourceLayerName'],
                        axis=1)




                    merged_df['TargetDB'] = merged_df.apply(
                        lambda row: '' if  row['TargetObjectType'] in ['select_list','with_cte'] else row['TargetDB'],
                        axis=1)

                    merged_df['TargetSchema'] = merged_df.apply(
                        lambda row: '' if  row['TargetObjectType']  in ['select_list','with_cte'] else row['TargetSchema'],
                        axis=1)

                    merged_df['TargetTable'] = merged_df.apply(
                        lambda row: '' if row['TargetObjectType']  in ['select_list','with_cte']  else row['TargetLayerName'],
                        axis=1)

                    merged_df=merged_df[[ 'SourceLayerName',
                                          'SourceDB',
                                          'SourceSchema',
                                          'SourceTable',
                                          'SourceObjectType',
                                          'SourceColumn',
                                          'TargetLayerName',
                                          'TargetDB',
                                          'TargetSchema',
                                          'TargetTable',
                                          'TargetColumn',
                                          'TargetObjectType'
                                         ]]

                    merged_df["ModelName"]=workBookName
                    merged_df["ReportName"] = sheetName if sheetName!='' else workBookName+'_'+sheetID
                    merged_df["ReportPath"] = workBookPath+'/'+sheetName if sheetName != '' else  workBookPath+'/'+\
                                                                                                  workBookName + '_' + sheetID
                    merged_df["TargetSQL"]=SqlText




                    listOfdf.append(merged_df)




                except Exception as e:
                    print(e)


            except Exception as e:
                  print(e)

        return pd.concat(listOfdf)

    def CreateTablesdf(self, dictResult):
        data_dict = dictResult["dlineage"]
        table_data = []
        column_data = []
        listOfdfColumns = []
        tableColumns_df=pd.DataFrame()
        try:
            for table in [data_dict["table"]]:
                if type(table)==list:
                    for tbl in table:
                        if not tbl['@name'].startswith("pseudo"):

                            table_id = tbl['@id']
                            table_name = tbl['@name']
                            table_schema = tbl['@schema']
                            table_db = tbl['@database']
                            table_type = tbl['@type']
                            table_alias = tbl['@alias']
                            table_coordinate= tbl['@coordinate']

                            table_data.append([table_id, table_name, table_schema, table_db,table_type,
                                               table_alias,table_coordinate
                                               ])

                            columns = tbl['column']
                            for column in columns:
                                column_id = column['@id']
                                column_name = column['@name']
                                column_coordinate = column['@coordinate']
                                column_data.append(
                                    [table_id, table_name, table_schema, table_db,table_type,
                                     table_alias, table_coordinate,
                                     column_id, column_name, column_coordinate])
                                column_df = pd.DataFrame(column_data,
                                                         columns=['TableID', 'TableName', 'TableSchema', 'TableDB',
                                                                  'TableType','TableAlias','TableCoordinate',
                                                                  'ColumnID',
                                                                  'ColumnName', 'ColumnCoordinate'])
                                column_data=[]
                                listOfdfColumns.append(column_df)
                else:
                    if not table['@name'].startswith("pseudo"):

                        table_id = table['@id']
                        table_name = table['@name']
                        table_schema = table['@schema']
                        table_db = table['@database']
                        table_type = table['@type']
                        table_alias = table['@alias']
                        table_coordinate = table['@coordinate']

                        table_data.append([table_id, table_name, table_schema, table_db, table_type,
                                           table_alias, table_coordinate
                                           ])

                        columns = table['column']
                        for column in columns:
                            column_id = column['@id']
                            column_name = column['@name']
                            column_coordinate = column['@coordinate']
                            column_data.append(
                                [table_id, table_name, table_schema, table_db, table_type,
                                 table_alias, table_coordinate,
                                 column_id, column_name, column_coordinate])
                            column_df = pd.DataFrame(column_data,
                                                     columns=['TableID', 'TableName', 'TableSchema', 'TableDB',
                                                              'TableType', 'TableAlias', 'TableCoordinate',
                                                              'ColumnID',
                                                              'ColumnName', 'ColumnCoordinate'])
                            column_data = []
                            listOfdfColumns.append(column_df)

            tableColumns_df = pd.concat(listOfdfColumns)
            return tableColumns_df
        except Exception as e:
            print(e)

    def CreateResultdf(self, dictResult):
        data_dict = dictResult["dlineage"]
        table_data = []
        column_data = []
        listOfdfColumns = []
        tableColumns_df = pd.DataFrame()
        if isinstance(data_dict["resultset"], list):
            elements=data_dict["resultset"]
        else:
            elements = [data_dict["resultset"]]



        try:
            for rs in elements:
                rs_id = rs['@id']
                rs_name = rs['@name']
                rs_type = rs['@type']
                rs_coordinate = rs['@coordinate']

                table_data.append([rs_id, rs_name, rs_type, rs_coordinate
                                   ])

                columns = rs['column']
                if  isinstance(columns, list):
                    for column in columns:
                        column_id = column['@id']
                        column_name = column['@name']
                        column_coordinate = column['@coordinate']
                        column_data.append(
                            [rs_id, rs_name, rs_type, rs_coordinate,
                             column_id, column_name, column_coordinate])
                        column_df = pd.DataFrame(column_data,
                                                 columns=['RsID', 'RsName', 'RsType', 'RsCoordinate',
                                                          'ColumnID',
                                                          'ColumnName', 'ColumnCoordinate'])
                        column_data = []
                        listOfdfColumns.append(column_df)
                else:
                    column_id = columns['@id']
                    column_name = columns['@name']
                    column_coordinate = columns['@coordinate']
                    column_data.append(
                        [rs_id, rs_name, rs_type, rs_coordinate,
                         column_id, column_name, column_coordinate])
                    column_df = pd.DataFrame(column_data,
                                             columns=['RsID', 'RsName', 'RsType', 'RsCoordinate',
                                                      'ColumnID',
                                                      'ColumnName', 'ColumnCoordinate'])
                    column_data = []
                    listOfdfColumns.append(column_df)



            tableColumns_df = pd.concat(listOfdfColumns)
            return tableColumns_df
        except Exception as e:
            print(e)

    def CreateRelationshipdf(self, dictResult):
        data_dict = dictResult["dlineage"]
        table_data = []
        sourceToTargetList = []
        listOfdf=[]

        tableColumns_df = pd.DataFrame()



        try:
            i=0
            for rl in data_dict["relationship"]:

                rl_id = rl['@id']
                rl_type = rl['@type']
                rl_effectType = rl['@effectType'] if '@effectiveType' in rl else ''

                targetId=rl['target']['@id']
                targetColumn = rl['target']['@column']
                targetParentID = rl['target']['@parent_id']
                targetParentName = rl['target']['@parent_name']
                targetCoordinate = rl['target']['@coordinate']

                if isinstance(rl['source'], list):
                    for src in rl['source']:
                        sourceId = src['@id']
                        sourceColumn =src['@column']
                        sourceParentID = src['@parent_id']
                        sourceParentName = src['@parent_name']
                        sourceCoordinate = src['@coordinate']
                        sourceToTargetList.append(
                        [rl_id,rl_type,rl_effectType,targetId,targetColumn,targetParentID,
                        targetParentName,targetCoordinate,sourceId,sourceColumn,sourceParentID,
                        sourceParentName,sourceCoordinate])
                        sourceToTargetdf = pd.DataFrame(sourceToTargetList,
                            columns=['rl_id', 'rl_type', 'rl_effectType', 'targetId','targetColumn',
                                     'targetParentID','targetParentName','targetCoordinate','sourceId',
                                     'sourceColumn','sourceParentID','sourceParentName','sourceCoordinate']
                            )
                        sourceToTargetList=[]

                        listOfdf.append(sourceToTargetdf)
                        i=i+1
                else:

                    sourceId = rl['source']['@id']
                    sourceColumn = rl['source']['@column']
                    sourceParentID = rl['source']['@parent_id']
                    sourceParentName = rl['source']['@parent_name']
                    sourceCoordinate = rl['source']['@coordinate']
                    sourceToTargetList.append(
                        [rl_id, rl_type, rl_effectType, targetId, targetColumn, targetParentID,
                         targetParentName, targetCoordinate, sourceId, sourceColumn, sourceParentID,
                         sourceParentName, sourceCoordinate])
                    sourceToTargetdf = pd.DataFrame(sourceToTargetList,
                                                    columns=['rl_id', 'rl_type', 'rl_effectType', 'targetId',
                                                             'targetColumn',
                                                             'targetParentID', 'targetParentName', 'targetCoordinate',
                                                             'sourceId',
                                                             'sourceColumn', 'sourceParentID', 'sourceParentName',
                                                             'sourceCoordinate']
                                                    )
                    sourceToTargetList = []

                    listOfdf.append(sourceToTargetdf)
                    i = i + 1

                    tableColumns_df = pd.concat(listOfdf)

            return tableColumns_df
        except Exception as e:
            print(e)


    def GetParsedResultPerQuery(self,Sqltext,dbvVendor):
        try:
            with open('Config.json') as file:
                # Load the JSON data into a dictionary
                data = json.load(file)
            url =data["GspUrl"]


            payload = json.dumps({
                "query": Sqltext,
                "vendor": dbvVendor
            })
            headers = {
                'Content-Type': 'application/json'
            }

            response = requests.request("POST", url, headers=headers, data=payload)

            dictreult=xmltodict.parse(response.text)
            return dictreult

        except Exception as e:
            print(e)


    def JoinWorkbookName(self,dfWb,dfMain):
        try:
         if  len(dfWb)>0 and len(dfMain)>0:
            merged_df = dfMain.merge(dfWb, left_on='workbookId', right_on='workbookID')[['workbookId','name','path','sheetID','sheetName']]

         return  merged_df
        except Exception as e:
            print(e)

    def JoinSheetIdAndElementID(self,dfSheet,dfElement):
        try:

            if len(dfSheet)>0 and len(dfElement)>0:

                merged_df = dfSheet.merge(dfElement, left_on=['sheetID','workbookId'], right_on=['sheetId','workbookId'])[
                    ['workbookId', 'name', 'path', 'sheetID', 'sheetName',"id"]]

                merged_df = merged_df.rename(columns={'name': 'WorkBookName',"id":"elementID",
                                                      'path':"WorkBookPath"
                                                      })


                return merged_df
            else:
                return None


        except Exception as e:
            print(e)

    def JoinElemetsAndSQL(self, dfElement, dfQueries):
        try:
            if len(dfElement)>0 and len(dfQueries)>0:
                merged_df = dfElement.merge(dfQueries, left_on=['elementID'], right_on=['elementId' ])[
                    ['workbookId', 'WorkBookName', 'WorkBookPath', 'sheetID', 'sheetName', "elementID","sql"]]
                return merged_df
        except Exception as e:
            print(e)


pg=Program()
pg.GetDataFrames("E:\\temp\\Sigma_22-06-2023-16-29-34")