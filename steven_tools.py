# tools for Trade
# 
# 
import re
import os



getCurrentDir = lambda: \
    os.path.dirname(os.path.abspath(__file__))



def getFileName(filename):
    """
        Return filename with path
    """
    return os.path.join(getCurrentDir(), filename)
    


def dumpFileContent(filename):
    """
        Return file contents
    """
    lines = []
    f = open(filename, 'r')
    lines = f.read().splitlines()
    f.close()
    return lines



def extractTradeFile(filename):
    """
        Return data before remove NameSpace line or fix TranscationRecords missing line.
    """
    lines = ""
    with open(filename, 'r') as f:
        lines = [line.strip("\n") for line in f]
    if lines[0].startswith('<GenevaLoader'):
        return lines[1:len(lines)-1]
    else:
        return ['<TransactionRecords>'] + lines + ['</TransactionRecords>']



def printReadable(dataset):
    print("Transcation(s): ", len(dataset))
    for index, x in enumerate(dataset):
        j = x[1]
        for index2, ii in enumerate(x[0]):
            if type(j[index2]) == list:
                for index3, m in enumerate(j[index2][0]):
                    msg = "trades[{}]['{}']['{}'] = '{}'"
                    print(msg.format(index, ii, m, j[index2][1][index3]))
            else:
                msg = "trades[{}]['{}'] = '{}'"
                print(msg.format(index, ii, j[index2]))
    return
    


def getTrade(data):
    dataset = []
    
    if "<TransactionRecords>" not in str(data):
        return dataset

    itemCount = 0
    passtag = ['<TransactionRecords>', '</TransactionRecords>']
    start_transcation_type = \
        [ '<Sell_New>', '<Buy_New>', '<SellShort_New>', '<CoverShort_New>'
        , '<ForwardFX_New>', '<Sell_Delete>', '<Buy_Delete>', '<ReverseRepo_InsertUpdate>'
        , '<Repo_InsertUpdate>', '<Repo_Delete>', '<Bond_InsertUpdate>']


    end_transcation_type = \
        [ '</Sell_New>', '</Buy_New>', '</SellShort_New>', '</CoverShort_New>'
        , '</ForwardFX_New>', '</Sell_Delete>', '</Buy_Delete>', '</ReverseRepo_InsertUpdate>'
        , '</Repo_InsertUpdate>', '</Repo_Delete>', '</Bond_InsertUpdate>']


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
            indices.append('transcation_type')
            details.append(re.sub('[<>]','', s))
        elif s in end_transcation_type: # Transcation end
            dataset.append([indices, details])
        else: # Process data 
            index = s.find(">")
            endindex = s.find("</")
            if endindex == -1:
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
#                print(index, i[1][index1], i[1][index2])
                dataset.remove(dataset[index])
                deleteKeyValue.append(i[1][index2])
#        print(deleteKeyValue)
    for index, i in enumerate(dataset):
        for index1, j in enumerate(i[0]):
            if index1 >-1 and j == 'KeyValue' and i[1][index1] in deleteKeyValue:
                dataset.remove(dataset[index])
 #               print(index, j, index1, i[1][index1])
    return dataset
