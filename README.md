# ES Report Generator (v1.0X) #


* Quick summary: This tool is to generate current elasticsearch indices report in csv file
* Version: 1.0

### How do I get set up? ###

* Make sure you have python 3.8 or later installed
* Install: 

        $ ./install.sh
        
* Help: 

        $ esreportgen -h
        
* Usage: 

        esreportgen https://localhost:9200 -u elastic -p secret 
    
    or
    
        esreportgen localhost -u elastic -p secret -P 9200 -s https -o ~/myesreports

### Contribution guidelines ###

* Writing tests: Repo owner
* Code review: Repo owner
* Other guidelines: Repo owner

### Who do I talk to? ###

* Repo owner or admin