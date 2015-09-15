Description
-----------
VDJMapReduce is a version of VDJMut that works with the MapReduce framework to process fastapair files in a hadoop distributed filesystem.

Fastapair input files 
---------------
Fastapair files are generated by the [USEARCH](http://www.drive5.com/usearch/) commandline tool.
The program also requires the number of times each sequence should be counted.

    >1 Cts=329
    GGCTGAGCTGGTGAGGCCTGGGTCTTCAGTGAAGCTGTCCTGCAAGGCTTCTGGCTAC
    >mIGHV1-61*01
    GGCTGAGCTGGTGAGGCCTGGGTCTTCAGTGAAGCTGTCCTGCAAGGCTTCTGGCTAC
    
    >3 Cts=235 
    AGCTGAGCTGATGAAGCCTGGGGCCTCAGTGAAGCTTTCCTGCAAGGCTACTGGCTAC
    >mIGHV1-9*01
    AGCTGAGCTGATGAAGCCTGGGGCCTCAGTGAAGCTTTCCTGCAAGGCTACTGGCTAC
 

