# Enhance Catalog Datasets with linked data
Given OCLC numbers, this Python script generates a 'Result.csv' file and an "errors.txt". In 'Result.csv', each row has an OCLC number, the LCSH links, GeoName links, VIAF links and types for the creator(s), contributer(s), and subject(s). If any errors occur while downloading .rdf files, the OCLC number will be added to "errors.txt". The script could also generate reports for given OCLC numbers' creators, contributors, and subjects seperatly in three .txt files.  
## Requirements
- Python 3
- A .txt file of OCLC numbers (example, "test_100.txt")
## Usage
1. Put the scirpt(**"GET_LCSH_VIAF.py"**) and your input .txt file (**[TXT]**) in the same folder
2. Create two folders to  store 
    - **[FOLDER1]** ("Item") the .rdf files of item records;
    - **[FOLDER2]** ("Sub") the .rdf files of the information of subjects
    - (files in the 'Item' and 'Sub' folders are .rdf files for OCLC numbers in the text_100.txt)
3. If you would like to see reports, create a folder and name it 'Report'.
4. Considering the input .txt file could be very large, the scirpt could chunk the OCLC numbers to several parts. The default **[SIZE]** of each part is 10000.
5. Reports for the creators, contributers, and subjects are optional. If you would like to see the reports for creators and contributors, put **'T'** after [SIZE]; if not, put **'F'**. Similary, the report for the subjects will be generated by adding **'T'** in the end of the command; adding **'F'** will not generate the subject report.

### Run
python3   GET_LCSH_VIAF.py   [TXT]   [FOLDER1] [FOLDER2] ([SIZE], default is 10000) T/F  T/F
