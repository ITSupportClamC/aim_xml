# tools for Trade
# 
# 
import re
import os
from itertools import chain
import logging
logger = logging.getLogger(__name__)



getCurrentDir = lambda: \
    os.path.dirname(os.path.abspath(__file__))



def getFileName(filename):
    """
        Return filename with path
    """
    return os.path.join(getCurrentDir(), filename)



def fileToLines(file):
    """
    [String] file => [List] lines in the file

    read a file in text mode and returns all its lines
    """
    lines = []
    with open(file, 'r') as f:
        lines = f.read().splitlines()

    return lines



def changeLines(lines):
    """
    [List] lines => [Iterable] lines
    """
    if lines[0].startswith('<GenevaLoader') and len(lines) < 4:
        logger.error('changeLines(): not enough number of lines')
        raise ValueError

    return chain( ['<Dummy_Record_Header>'] 
                , lines[2:-2] if lines[0].startswith('<GenevaLoader') else lines
                , ['</Dummy_Record_Header>'])



def getTrade(data):
    dataset = []
    itemCount = 0
  
    passtag = ['<Dummy_Record_Header>', '</Dummy_Record_Header>']

    start_transcation_type = \
        [ '<Sell_New>', '<Buy_New>', '<SellShort_New>', '<CoverShort_New>'
        , '<ForwardFX_New>', '<Sell_Delete>', '<Buy_Delete>', '<ReverseRepo_InsertUpdate>'
        , '<Repo_InsertUpdate>', '<Repo_Delete>', '<Bond_InsertUpdate>'
        , '<VariableRateRecord>']


    end_transcation_type = \
        [ '</Sell_New>', '</Buy_New>', '</SellShort_New>', '</CoverShort_New>'
        , '</ForwardFX_New>', '</Sell_Delete>', '</Buy_Delete>', '</ReverseRepo_InsertUpdate>'
        , '</Repo_InsertUpdate>', '</Repo_Delete>', '</Bond_InsertUpdate>'
        , '</VariableRateRecord>']


    indices = []
    details = []
    indices2 = []
    details2 = []
    level = 0
    levelFieldName = "";
    
    for s in data:
        if s in passtag:
            pass
        elif s in start_transcation_type: # New transcation
            level = 0
            indices = []
            details = []
            indices2 = []
            details2 = []
            indices.append('transaction_type')
            details.append(re.sub('[<>]','', s))
        elif s in end_transcation_type: # Transcation end
            dataset.append([indices, details])
        else: # Process data 
            index = s.find(">")
            endindex = s.find("</")
            simplytagindex = s.find("/>")
            if simplytagindex >= 0:
                field = s[s.find("<")+1:simplytagindex]
                indices.append(field.strip())
                details.append("")
            elif endindex == -1:
                level = 1
                s = re.sub('[<>]','', s)
                indices2 = []
                details2 = []
                levelFieldName = s
            elif endindex == 0:
                s = re.sub('[</>]','', s)
                indices.append(s)
                details.append([indices2, details2])
            else:
                field = s[s.find("<")+1:index]
                if level == 1:
                    indices2.append(field)
                    if index + 1 == endindex:
                        details2.append("")
                        data = ""
                    else:
                        data = s[index+1:endindex]
                        details2.append(data)
                else:
                    indices.append(field)
                    if index + 1 == endindex:
                        details.append("")
                        data = ""
                    else:
                        data = s[index+1:endindex]
                        details.append(data)
    return dataset
    


def getDeleteTrade(dataset):
    deleteKeyValue = []
    
    for index, i in enumerate(dataset):
        for j in i[0]:
            index1 = j.find('transcation_type')
            if index1 > -1 and i[1][index1].find('_Delete') > -1: 
                index2 = j.find('KeyValue')
                keyValue = i[1][index2]
                dataset.remove(dataset[index])
                deleteKeyValue.append(i[1][index2])
    for index, i in enumerate(dataset):
        for index1, j in enumerate(i[0]):
            if index1 >-1 and j == 'KeyValue' and i[1][index1] in deleteKeyValue:
                dataset.remove(dataset[index])
    return dataset