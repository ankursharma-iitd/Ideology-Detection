from mytree import *
from treeUtil import *
# from pycorenlp import StanfordCoreNLP


val = {'pro':0,'anti':1,'default':-1}


# pro:1, anti:0, neutral:2
val_all = {'pro':1,'anti':0,'default':-1,'neutral':2}
# neutral:0, non-neutral:1
val_neutral = {'neutral':0, 'non_neutral':1}


def convert(T):
    label = val[T.label] if (hasattr(T,'label')) else None
    print(label)
    newTree = convert_primary_new(T,label)
    annotate_all(newTree)

    return newTree

def convert_neutral(T):
    label = val_neutral[T.label] if (hasattr(T,'label')) else None
    print(label)
    newTree = convert_primary_new_all(T,label)
    annotate_all(newTree)

    return newTree

def convert_all(T):
    label = val_all[T.label] if (hasattr(T,'label')) else None
    print(label)
    newTree = convert_primary_new_all(T,label)
    annotate_all(newTree)

    return newTree


def convert_primary(T):
    if (hasattr(T,'label')):
        print(T.label) 
    label = val[T.label] if (hasattr(T,'label')) else None
    # label = val[T.label] if (hasattr(T,'label') ) else None # changed for ignoring neutral

    if isinstance(T,leafObj):
        newTree = Node(label,T.word,T.pos)
        newTree.isLeaf = True
        return newTree
    else:
        newTree = Node(label)
    
    leftChild = convert_primary(T.c1)
    rightChild = convert_primary(T.c2)
    leftChild.parent = newTree
    rightChild.parent = newTree

    newTree.left = leftChild
    newTree.right = rightChild

    return newTree

def convert_primary_new(T,label):
    # from IPython import embed; embed()
    if T is None:
        return None
    # label = val[T.label] if (hasattr(T,'label') ) else None # changed for ignoring neutral
    T.set_label(label)
    # if (T.isLeaf) : print (T.word)

    T.left = convert_primary_new(T.left,label)
    T.right = convert_primary_new(T.right,label)

    return T

    # if T.isLeaf:
    #     newTree = Node(label,T.word,T.pos)
    #     newTree.isLeaf = True
    #     return newTree
    # else:
    #     newTree = Node(label)
    
    # leftChild = convert_primary_new(T.left)
    # rightChild = convert_primary_new(T.right)
    # leftChild.parent = newTree
    # rightChild.parent = newTree

    # newTree.left = leftChild
    # newTree.right = rightChild

    # return newTree

def convertNLTK_tree_primary(tree):
    if tree.height()==2:
        newTree = Node('default',tree[0],None)
        newTree.isLeaf = True
        return newTree
    newTree = Node('default')
    leftChild = convertNLTK_tree_primary(tree[0])
    rightChild = convertNLTK_tree_primary(tree[1])
    
    leftChild.parent = newTree
    rightChild.parent = newTree

    newTree.left = leftChild
    newTree.right = rightChild

    return newTree

def convertNLTK_tree(tree):
    return Tree(convertNLTK_tree_primary(tree))




def annotate_all(T):
    if T == None: return
    if T.label != None : 
        T.annotated = True
    else:
        T.annotated = False
        T.set_label(T.parent.label)
    annotate_all(T.left)
    annotate_all(T.right)

def buildBalTree(sent):
    words = sent.split(' ')

    nodes = words

    while len(nodes)>1:
        temp = []
        for i in range(0,len(nodes),2):
            lChild = Node(None,nodes[i],None) if isinstance(nodes[i],str) else nodes[i]
            if i+1<len(nodes):
                rChild = Node(None,nodes[i+1],None) if isinstance(nodes[i+1],str) else nodes[i+1]
            else:
                rChild = None
            if isinstance(nodes[i],str):
                lChild.isLeaf = True
                if rChild is not None:
                    rChild.isLeaf = True
            newNode = Node(None)
            lChild.parent = newNode
            newNode.left = lChild
            newNode.right = rChild
            if rChild is not None:
                rChild.parent = newNode
            temp.append(newNode)
        nodes=temp
    return Tree(nodes[0])

def readFile2Trees(filename):
    trees = []
    with open(filename,'r') as file:
        for line in file:
            if line=='\n':
                continue
            else:
                [labelname,sent] = line.split(': ',1)
                tree = buildBalTree(sent)
                tree.root.set_label(val[labelname])
                if val[labelname]!=2:
                    trees.append(tree)
    return trees
    
            
                






