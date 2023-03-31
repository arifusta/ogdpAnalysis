from zipfile import ZipFile
from fdDiscovery import *
from dataclasses import dataclass, field


@dataclass
class decompTableStat:
    tableID : str
    orgNumColumns : int
    orgCellCount : int
    orgUniqueScore : float = -1
    hasDecomposition : bool = False
    decompCellCount : int = 0
    cellCountReduction : float = 0
    numberofCommonAttributes : int = 0
    numberofUncommonAttributes : int = 0
    uqScoreIncreaseInOnlyOnce : float = 0
    uqScoreIncreaseBest : float = 0
    uqScoreIncreaseWorst : float = 0
    havingKeyColumnRatio : float = 0
    decompNumColumns : int = 0
    numberofKeyColumnsDecomp : int = 0

@dataclass
class ColumnStat:
    tableID: str
    colIndex : int
    colName : str = ""
    missValueCount : int = 0
    missRatio : float = 0
    uniqueValueCount : int = 0
    uniqueScore : float = 0
    isKey : bool = False
    tupleCount : int = 0
    isAllNull : bool = False
    colType : int = -1
    colTypeName : str = ""

@dataclass
class TableStat:
    tableID: str
    datasetID : str
    columnCount : int = 0
    rowCount : int = 0
    missRatio : float = 0.0
    uniquenessScore : float = 0.0
    emptyColumnCount : int = 0
    emptyColumnRatio : float = 0.0
    keyColumnCount : int = 0

@dataclass
class DecomposedTable:
    """Class for keeping track of an item in inventory."""
    id: int
    setOfAttributes: set
    funcDeps: list # functional dependencies inherited from the original table for this decomposed table
    inBNCF : bool = False # default it always requires to be checked.

@dataclass
class CSVTable:
    tableID: str
    numTuples : int = 0
    numColumns : int = 0
    redundantAttributesRatio : float = 0
    numberofFDs: int = 0
    foundFDs: list = field(default_factory=list) # list of minimal fds found in the original table
    compositeKeyLength : int = 0
    candidateKeys: set = field(default_factory=set) # set of attribute subsets whose closures cover entire column set minimally.
    hasMinimalNonKeyFD : bool = False
    numberofDecomposedTables : int = 1
    listofDecomposedTables : list = field(default_factory=list) # list of decomposed tables


def readCSVFromZipUS(table_doc):
    zipUK = ZipFile('us.zip')

    csvID = table_doc['uuid']
    encoding = table_doc['encoding']
    header = table_doc['header']
    delimiter = table_doc['delimiter']
    try:
        with zipUK.open(f'files/{csvID}.csv') as myZip:
            if header == 0:
                df = pd.read_csv(myZip, low_memory=False, encoding=encoding, delimiter=delimiter)
            else:
                df = pd.read_csv(myZip, skiprows=header, low_memory=False, encoding=encoding, delimiter=delimiter)

            df.drop(df.filter(regex="Unnamed"),axis=1, inplace=True)
            df.dropna(how='all', inplace= True)
            return {"uuid": csvID, "df": df}
    except:
        return

def readCSVFromZipUK(table_doc):
    zipUK = ZipFile('uk.zip')

    csvID = table_doc['uuid']
    encoding = table_doc['encoding']
    header = table_doc['header']
    delimiter = table_doc['delimiter']
    try:
        with zipUK.open(f'files/{csvID}.csv') as myZip:
            if header == 0:
                df = pd.read_csv(myZip, low_memory=False, encoding=encoding, delimiter=delimiter)
            else:
                df = pd.read_csv(myZip, skiprows=header, low_memory=False, encoding=encoding, delimiter=delimiter)

            df.drop(df.filter(regex="Unnamed"),axis=1, inplace=True)
            df.dropna(how='all', inplace= True)
            return {"uuid": csvID, "df": df}
    except:
        return

def readCSVFromZipCA(table_doc):
    zipUK = ZipFile('opencanada-tables.zip')

    csvID = table_doc['uuid']
    encoding = table_doc['encoding']
    header = table_doc['header']
    #delimiter = table_doc['delimiter']
    try:
        with zipUK.open(f'files/{csvID}.csv') as myZip:
            if header == 0:
                df = pd.read_csv(myZip, low_memory=False, encoding=encoding)
            else:
                df = pd.read_csv(myZip, skiprows=header, low_memory=False, encoding=encoding)

            #df.columns.str.match("Unnamed")
            df.drop(df.filter(regex="Unnamed"),axis=1, inplace=True)
            df.dropna(how='all', inplace= True)
            return {"uuid": csvID, "df": df}
    except:
        return

def readCSVFromZipSG(table_doc):
    zipUK = ZipFile('sg-tables.zip')

    csvID = table_doc['uuid']
    encoding = table_doc['encoding']
    header = table_doc['header']
    #delimiter = table_doc['delimiter']
    try:
        with zipUK.open(f'files/{csvID}.csv') as myZip:
            if header == 0:
                df = pd.read_csv(myZip, low_memory=False, encoding=encoding)
            else:
                df = pd.read_csv(myZip, skiprows=header, low_memory=False, encoding=encoding)

            df.drop(df.filter(regex="Unnamed"),axis=1, inplace=True)
            df.dropna(how='all', inplace= True)
            return {"uuid": csvID, "df": df}
    except:
        return

def findAttributeClosures(listofColumns, listoffds):

    closureDict = {} # attribute-closers sets of attributes as key-value dict
    redundantAttrSet = set()
    for col in listofColumns:
        tmpSet = set()
        tmpSet.add(col)
        closureDict[frozenset([col])] = tmpSet

    for fd in listoffds:
        lhsSet = fd[0]
        rhsSet = fd[1]

        if len(lhsSet) == 1: # only add attributes on rhs to the dictionary
            closureDict[frozenset(lhsSet)].update(rhsSet)
            if len(rhsSet) + 1 != len(listofColumns):
                redundantAttrSet.update(rhsSet)
        else: # add lhs to dict, next add rhs to dict and iterate over attributes on rhs
            tmpSet = set()
            tmpSet.update(lhsSet)
            tmpSet.update(rhsSet)

            for rCol in rhsSet:
                tmpSet.update(closureDict[frozenset([rCol])])

            closureDict[frozenset(lhsSet)] = tmpSet

    return closureDict, redundantAttrSet

def extractFDs(tablePair):
    tableID = tablePair[0]
    table_df = tablePair[1]

    if table_df.shape[1] == 0:
        return

    currentFds, minKeyLength, keySets = getFunctionalDependencies(table_df)  # dictionary of fds where keys are levels
    listofFds = [fd for vals in currentFds.values() for fd in vals]
    attributeClosures, redundantAttrSet = findAttributeClosures(table_df.columns, listofFds)

    procTable = CSVTable(tableID) # initalize processed table dataclass object
    procTable.foundFDs = listofFds # set list of fds
    procTable.numberofFDs = len(listofFds)
    procTable.compositeKeyLength = minKeyLength # set minimum composite key length
    procTable.numTuples = table_df.shape[0]
    procTable.numColumns = table_df.shape[1] # set number of columns
    procTable.candidateKeys = keySets
    procTable.redundantAttributesRatio = len(redundantAttrSet) / procTable.numColumns

    hasNonKeyFDFlag = False
    decomposedCount = 0
    tempRelation = DecomposedTable(decomposedCount, set(table_df.columns), listofFds)
    decompositionCandidates = deque()  # sets of attributes in the relation R
    decompositionCandidates.append(tempRelation)  # add inital relation into the stack
    bcnfDecomposition = [] # list to store subsets of decomposed tables if found any


    while (len(decompositionCandidates) != 0):
        currentRelation = decompositionCandidates.pop()
        if len(currentRelation.funcDeps) == 0:
            decomposedCount += 1
            bcnfDecomposition.append(currentRelation)
            continue
        inBCNF = True
        decomposedFDs = []  # initalize empty list of lists for fds for the corresponding decomposed table
        decomposedAtts = set()  # initalize empty list of sets for attributes for the corresponding decomposed table
        for fd in currentRelation.funcDeps:
            lhsSet = fd[0]
            closureSet = fd[1]
            unionofFd = lhsSet.union(closureSet)

            if frozenset(lhsSet) in keySets: # if lhs is a candidate key or a super key determining entire set of attributes, it cannot violoate bcnf, skip
                continue
            else:
                lhsClosure = attributeClosures[frozenset(lhsSet)]
                if currentRelation.setOfAttributes.issubset(lhsClosure): # closure of lhs contains entire set of columns for the current relation, hence it is superkey,
                    # does not violate BCNF, skip the fd from further processing
                    continue

                if not hasNonKeyFDFlag:
                    hasNonKeyFDFlag = True

            if not inBCNF:  # one of the earlier FDs is found to be violating BCNF,
                # so put fds coming after into 2 lists of fds for decomposition
                if lhsSet.issubset(decomposedAtts) and len(closureSet.intersection(decomposedAtts)) > 0:
                    decomposedFDs.append((lhsSet, closureSet.intersection(decomposedAtts)))
            else: # up until now the relation was not violating BNCF
                inBCNF = False
                # we found the fd violating BCNF, split table into 2

                bcnfRelation = DecomposedTable(decomposedCount, unionofFd, [fd])
                decomposedCount += 1
                bcnfDecomposition.append(bcnfRelation)
                decomposedAtts = currentRelation.setOfAttributes.difference(closureSet)


        if not inBCNF:
            tempRelation = DecomposedTable(decomposedCount, decomposedAtts, decomposedFDs)
            decompositionCandidates.append(tempRelation)
        elif len(currentRelation.funcDeps) != 0:
            bcnfDecomposition.append(currentRelation)

    procTable.hasMinimalNonKeyFD = hasNonKeyFDFlag
    procTable.numberofDecomposedTables = len(bcnfDecomposition)
    procTable.listofDecomposedTables = bcnfDecomposition

    return procTable


#table_df = readCSVFile('testRelation1.csv')
#result = extractFDs(('1', table_df))
