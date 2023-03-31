import itertools

class Level:
    def __init__(self, level):
        self.level = level
        self.attrSets = []
        self.funcDependencies = []

    def addCandidate(self, candidate):
        self.attrSets.append(candidate)

    def addFD(self, lhs, rhs):
        self.funcDependencies.extend((lhs, rhs))

class lhsCandidate:

    def __init__(self, listOfAttributes, level):
        self.level = level
        self.cardinality = -1
        self.candidate = listOfAttributes
        self.isFreeSet = False
        self.closure = []
        self.quasiClosure = []
        #self.maxIndex = -1
        self.partition = []
        self.isKey = False
        self.attributeSetKey = tuple([c.name for c in self.candidate])
        #self.fds = []
        self.hasMinimalDependencies = False
        self.isUseless = False

        if len(listOfAttributes) == 1: # single attribute candidate
            self.cardinality = listOfAttributes[0].cardinality
            #self.closure.append(listOfAttributes[0])
            self.quasiClosure.append(listOfAttributes[0])
            if listOfAttributes[0].isUseless: # the attribute only has single unique value or is entirely empty
                self.isFreeSet = False
                self.isUseless = True
            else:
                self.isFreeSet = True
            self.maxIndex = listOfAttributes[0].index
            self.attributeSetKey = tuple([listOfAttributes[0].name])
            self.partition = listOfAttributes[0].partition
            if listOfAttributes[0].isKey:
                self.isKey = True
                self.hasMinimalDependencies = True

    def getSetofColumnNames(self):
        return set([c.name for c in self.candidate])

    def getSetofColumnNamesInClosure(self):
        return set([c.name for c in self.closure if c not in self.candidate])

    def generateMaxSubsets(self):
        attrNameList = [c.name for c in self.candidate]
        return list(itertools.combinations(attrNameList, len(attrNameList)-1))

    def addFDRHS(self, attrRight):
        if isinstance(attrRight, list):
            self.fds.extend(attrRight)
        else:
            #add an attribute instance to the fd list
            self.fds.append(attrRight)
        if not self.hasMinimalDependencies:
            self.hasMinimalDependencies = True

    def calculatePartition(self, setPartition, columnPartition, tableLength): # calculate partition of the candidate using partitions of X and A where X is set of attributes and A is a column
        #each partition variable is a list of sets
        S = {}
        T = {}

        joinedPartition = []
        for classCount, eqClassSet in enumerate(setPartition):
            for tupIndex in eqClassSet:
                T[tupIndex] = classCount
            S[classCount] = set()

        intersectionCount = 0
        for classCount, eqClassSet in enumerate(columnPartition):
            for tupIndex in eqClassSet:
                if tupIndex in T:
                    S[T[tupIndex]].add(tupIndex)
            for tupIndex in eqClassSet:
                if tupIndex in T:
                    if len (S[T[tupIndex]]) >= 2:
                        joinedPartition.append(S[T[tupIndex]])
                        intersectionCount += len (S[T[tupIndex]])
                    S[T[tupIndex]] = set()

        self.cardinality = (tableLength - intersectionCount) + len(joinedPartition) # all singletion tuples + new set of partitions
        self.partition = [s for s in joinedPartition if len(s) >= 2]
        #[eqClassSet for eqClassSet in joinedPartition if len(eqClassSet)> 1]

        if self.cardinality == tableLength:
            self.isKey = True

    def joinCandidates(self, otherCandidate):
        #increase candidate length by 1 using join
        temp = []
        unionAttributes = set(self.candidate).union(set(otherCandidate.candidate))
        assert len(unionAttributes) == self.level + 1

        newCandidate = lhsCandidate(sorted(list(unionAttributes), key=lambda x: x.index), self.level + 1)
        return newCandidate

    def __repr__(self):
        lhsString = ','.join([c.name for c in self.candidate])
        return lhsString

    def __str__(self):
        lhsString = ','.join([c.name for c in self.candidate])
        return lhsString

    def __eq__(self, other):
        if isinstance(other, lhsCandidate):
            return self.attributeSetKey == other.attributeSetKey
        return False

    def __hash__(self):
        return hash (self.attributeSetKey)

class attribute:

    def __init__(self, name, index):
        self.name = name
        self.index = index
        self.isKey = False
        self.isUseless = False
        self.length = 0
        self.cardinality = 0
        self.partition = []

    def calculatePartition(self, values, tableLength):
        temp = {}

        NA_SET = set([
            "",
            "nan",
            "null",
            "n/a",
            "n/d",
            "-",
            "...",
            "(n/a)",
        ])

        for index, row in enumerate(values):
            if isinstance(row, list):
                val = tuple(row)
            else:
                val = row

            if str(val).lower() in NA_SET:
                continue

            if val in temp:
                temp[val].add(index)
            else:
                tempSet = set()
                tempSet.add(index)
                temp[val] = tempSet

        # get number of eq classes including every tuple
        #strip equivalance classes of size 1
        self.cardinality = len(temp)

        self.partition = [v for k, v in temp.items() if len(v) >= 2]
        #[val for key, val in temp.items() if len(val)> 1]

        if self.cardinality == tableLength:
            self.isKey = True

        if self.cardinality == 1 or self.cardinality == 0:
            self.isUseless = True

    def __eq__(self, other):
        if isinstance(other, attribute):
            return self.name == other.name

        return False

    def __hash__(self):
        return hash (self.name)

    def __repr__(self):
        return self.name

    def __str__(self):
        return str({'Name': self.name, 'Index': self.index, 'cardinality':self.cardinality, 'isKey':self.isKey})
