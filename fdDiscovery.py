import pandas as pd
from collections import defaultdict
from collections import deque
import numpy as NP
import sys
from fdUtil import *

def readCSVFile(filepath, encoding='UTF-8'):

    table_df = pd.read_csv(filepath, encoding=encoding)
    return table_df

def getColumnNames(table_df):
    temp = {}
    columnList = []
    for index, col in enumerate(table_df.columns):
        temp[col] = index
        attr = attribute(col, index)
        attr.calculatePartition(table_df[col].tolist(), table_df.shape[0])
        columnList.append(attr)

    return columnList, temp

def getPrevLevelClosure(prevLevel, currLevel, nonKeyAttributes):

    candDictCurrent = {k.attributeSetKey: v for v, k in enumerate(currLevel.attrSets)} # dictionary of unique tuple of attribute sets - index for current level
    candDictPrev = {k.attributeSetKey: v for v, k in
                       enumerate(prevLevel.attrSets)}  # dictionary of unique tuple of attribute sets - index for previous level

    for candidate in prevLevel.attrSets: # for each candidate in previous level
        if not candidate.isKey:
            candidate.closure.extend(candidate.quasiClosure)
            for A in nonKeyAttributes.difference(set(candidate.quasiClosure)): # for each attribute in R' \ cand.quasiClosure
                # A is a right hand side candidate
                if A.isUseless:
                    continue
                tempList = []
                tempList.extend(candidate.candidate)
                tempList.append(A)
                tempCandidate = lhsCandidate(sorted(tempList, key=lambda x: x.index), len(tempList))

                candCardinality = -1
                candTupleKey = tempCandidate.attributeSetKey

                if candTupleKey in candDictCurrent:
                    candCardinality = currLevel.attrSets[candDictCurrent[candTupleKey]].cardinality
                else:
                    subsetKeys = tempCandidate.generateMaxSubsets()
                    candCardinality = max([prevLevel.attrSets[candDictPrev[ssKey]].cardinality
                                           for ssKey in subsetKeys if ssKey in candDictPrev])

                if candidate.cardinality == candCardinality: #
                    #print(f'FD found: {candidate.attributeSetKey} -> {A.name}')
                    candidate.closure.append(A)
                    candidate.isFreeSet = False
                    #candidate.addFDRHS(A)

            if len(set(candidate.closure).difference(set(candidate.quasiClosure))) > 0:
                candidate.hasMinimalDependencies = True

def getCurrentLevelQuasiClosure(prevLevel, currLevel, listOfColumns):
    candDictPrev = {k.attributeSetKey: v for v, k in
                       enumerate(prevLevel.attrSets)}  # dictionary of unique tuple of attribute sets - index for previous level

    for candidate in currLevel.attrSets:  # for each candidate in current level
        candidate.quasiClosure.extend(candidate.candidate)
        freeSetFlag = True
        subsetKeys = candidate.generateMaxSubsets() # generate max subsets of the candidate in the current level
        for ssKey in subsetKeys:
            if ssKey in candDictPrev:
                candidate.quasiClosure.extend(prevLevel.attrSets[candDictPrev[ssKey]].closure)
                if candidate.cardinality == prevLevel.attrSets[candDictPrev[ssKey]]:  # candidate is not a free-set
                    freeSetFlag = False
        candidate.quasiClosure = sorted(list(set(candidate.quasiClosure)), key=lambda x: x.index)
        if freeSetFlag:
            candidate.isFreeSet = True
        if candidate.isKey:
            candidate.closure = listOfColumns
            candidate.hasMinimalDependencies = True
            #candidate.addFDRHS(listOfColumns)
            #print(f'FD found: {candidate.attributeSetKey} -> {tuple([c.name for c in listOfColumns])}')

def pruneCurrentLevel(currLevel, prevLevel):
    #delete candidates that are not free sets from the current level
    #before generating next level

    candDictPrev = {k.attributeSetKey: prevLevel.attrSets[v] for v, k in
                    enumerate(
                        prevLevel.attrSets)}  # dictionary of unique tuple of attribute sets - index for previous level

    freeSetCandidates = []
    for candidate in currLevel.attrSets:
        flag = True
        for index in range(len(candidate.candidate)):

            candAttributeKey = tuple([c.name for j,c in enumerate(candidate.candidate) if j != index])
            if candAttributeKey in candDictPrev:
                if candidate.cardinality == candDictPrev[candAttributeKey].cardinality:
                    flag = False
                    break

        if flag :
            freeSetCandidates.append(candidate)
    currLevel.attrSets = freeSetCandidates

def generateNextLevel(currentLevel, nonKeyAttributes, keySets, tableLength):

    nextLevel = Level(currentLevel.level + 1)
    if currentLevel.level == len(nonKeyAttributes):
        return nextLevel
    if len (currentLevel.attrSets) < 2: # not enough candidates in the current level to generate next level
        return nextLevel

    if currentLevel.level == 1: # only 1 attribute in current candidates, combine them without checking subsets.
        for indexI, cand in enumerate(currentLevel.attrSets):
            if cand.isKey or cand.isUseless:
                continue
            for indexJ, addCand in enumerate(currentLevel.attrSets[indexI + 1:]):
                if addCand.isKey or cand.isUseless:
                    continue
                newCandidate = cand.joinCandidates(addCand)
                #values = [list(row)[1:] for row in table_df.iloc[:, [a.index for a in newCandidate.candidate]].itertuples()]
                newCandidate.calculatePartition(cand.partition, addCand.partition, tableLength)
                if newCandidate.isKey:
                    keySets.add(frozenset([c.name for c in newCandidate.candidate]))

                nextLevel.addCandidate(newCandidate)
    else:
        freesetDictCurrent = {k.attributeSetKey: currentLevel.attrSets[v] for v, k in
                        enumerate(
                            currentLevel.attrSets)}  # dictionary of unique tuple of attribute sets - index for previous level

        tempNextCandidateSet = set()

        for indexI, cand in enumerate(currentLevel.attrSets):
            if cand.isKey:
                continue
            for indexJ, addCand in enumerate(currentLevel.attrSets[indexI + 1:]):
                if addCand.isKey:
                    continue
                if len (set(cand.candidate).intersection(set(addCand.candidate))) == currentLevel.level - 1: # pairs of candidates have level - 1 common attributes, perform join if not discard

                    newCandidate = cand.joinCandidates(addCand)
                    newCandSetofAttributes = set([c.name for c in newCandidate.candidate])
                    if frozenset(newCandSetofAttributes) in tempNextCandidateSet:
                        continue

                    isMinimal = True

                    for index in range(len(newCandidate.candidate)):

                        candAttributeKey = tuple([c.name for j, c in enumerate(newCandidate.candidate) if j != index])
                        if candAttributeKey not in freesetDictCurrent or freesetDictCurrent[candAttributeKey].isKey:
                            isMinimal = False
                            break

                    if not isMinimal:
                        continue

                    tempNextCandidateSet.add(frozenset(newCandSetofAttributes))

                    #values = [list(row)[1:] for row in
                    #          table_df.iloc[:, [a.index for a in newCandidate.candidate]].itertuples()]
                    rhsAttribute = list(set(newCandidate.candidate).difference(set(cand.candidate)))[0]
                    newCandidate.calculatePartition(cand.partition, rhsAttribute.partition, tableLength)
                    if newCandidate.isKey:
                        keySets.add(frozenset([c.name for c in newCandidate.candidate]))

                    nextLevel.addCandidate(newCandidate)
    nextLevel.attrSets = list(set(nextLevel.attrSets))
    return nextLevel

def displayFDS(currLevel, listofColumns, functDeps):
    if currLevel.level == 0:
        return 0, False
    numberOfFds = 0
    compositeKeyFlag = False
    #
    for candidate in currLevel.attrSets:
        if candidate.hasMinimalDependencies:
            #print(f'FD found: {candidate.attributeSetKey} -> {tuple([c.name for c in sorted(candidate.fds, key=lambda x: x.index)])}')
            if not compositeKeyFlag and set(candidate.closure) == set(listofColumns):
                compositeKeyFlag = True
                #candidate.isKey = True

            #rhsDifference = list(set(candidate.closure).difference(candidate.quasiClosure))
            #numberOfFds += len(rhsDifference)

            #rhsSet = tuple([c.name for c in sorted(rhsDifference, key=lambda x: x.index)])
            #for rhsAttribute in sorted(rhsDifference, key=lambda x: x.index):
            #if candidate.level == 1:
            #    lhsSet = candidate.candidate[0].name
            #    #print(f'{candidate.candidate[0].name} -> {rhsAttribute.name}')
            #else:
            #lhsSet = candidate.attributeSetKey
            #print(f'{candidate.attributeSetKey} -> {rhsAttribute.name}')
            diffSet = set(candidate.closure).difference(set(candidate.quasiClosure))
            functDeps[currLevel.level].append((candidate.getSetofColumnNames(),set([c.name for c in diffSet])))
            candidate.isFreeSet = False

    #print(f'Functional Dependencies found in Level {currLevel.level} are processed!')
    return numberOfFds, compositeKeyFlag

def getFunctionalDependencies(table_df):
    listOfColumns, columnIndexes = getColumnNames(table_df)
    #print(f'Number of columns in the table is {len(listOfColumns)}')
    #print(f'Number of tuples in the table is {table_df.shape[0]}')

    prevLevel = Level(0)
    firstLevel = Level(1)
    nonKeyAttributes = set()  # set of non-key attributes
    keySets = set()
    functionalDeps = {}
    functionalDeps[1] = []

    miminalCompositeKeyLength = 0

    for attr in listOfColumns:
        newCandidate = lhsCandidate([attr], 1)
        # attr.clearPartition() # remove partition from the attribute
        if not attr.isKey:  # if the attribute is non-key, add it into set of nonkey attributess
            nonKeyAttributes.add(attr)
        else:
            # newCandidate.addFDRHS(listOfColumns)
            newCandidate.closure = listOfColumns
            keySets.add(frozenset([c.name for c in newCandidate.candidate]))
            # print(f'FD found: {attr.name} -> {tuple([c.name for c in listOfColumns])}')
        firstLevel.addCandidate(newCandidate)

    # print(nonKeyAttributes)
    k = 1
    LEVELS = [prevLevel, firstLevel]

    while (len(LEVELS[k].attrSets) != 0):
        if k != 1:
            getPrevLevelClosure(LEVELS[k - 1], LEVELS[k], nonKeyAttributes)  # prevLevel , currLevel
            getCurrentLevelQuasiClosure(LEVELS[k - 1], LEVELS[k], listOfColumns)  # prevLevel , currLevel
            numfds, compositeFlag = displayFDS(LEVELS[k - 1], listOfColumns, functionalDeps)
            if compositeFlag and miminalCompositeKeyLength == 0:
                miminalCompositeKeyLength = k - 1

            pruneCurrentLevel(LEVELS[k], LEVELS[k - 1])  # prevLevel , currLevel

        #numberOfFds.append(numfds)
        # if k == 3:
        #     k += 1
        #     break
        LEVELS.append(generateNextLevel(LEVELS[k], nonKeyAttributes, keySets, table_df.shape[0]))  # currLevel
        functionalDeps[k+1] = []
        k += 1
        if k == 4:
            break

    numfds, compositeFlag = displayFDS(LEVELS[k - 1], listOfColumns, functionalDeps)
    #numberOfFds.append(numfds)
    if compositeFlag and miminalCompositeKeyLength == 0:
        miminalCompositeKeyLength = k - 1
    #print(f'Total number of FDs found: {numberOfFds}')
    return functionalDeps, miminalCompositeKeyLength, keySets

#table_df = readCSVFile('testRelation1.csv')
#fds = getFunctionalDependencies(table_df)

#temp = [1, 3, 5]
#values = [list(row)[1:] for row in table_df.iloc[:, temp].itertuples()]
#print (values)

#del table_df
#print(columnIndexes)

