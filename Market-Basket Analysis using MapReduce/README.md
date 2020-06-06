# Example
Sample I/O:

Input ->
```
cracker,bread,banana
cracker,coke,butter,coffee
cracker,bread
cracker,bread
cracker,bread,coffee
butter,coker
butter,coke,bread,cracker
```

Output ->
```
(banana, bread) 1
(banana, cracker)   1
(bread, butter) 1
(bread, coffee) 1
(bread, coke)   1
(bread, cracker)    5
(butter, coffee)    1
(butter, coke)  3
(butter, cracker)   2
(coffee, coke)  1
(coffee, cracker)   2
(coke, cracker) 2
```
# How To Run
```
hadoop jar mba.jar /sample_ip /output
```
