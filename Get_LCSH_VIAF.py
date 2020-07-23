import Lib.urllib.request
import rdflib.plugins.sparql as sparql
import rdflib
from itertools import islice
import itertools
import requests
import glob
import urllib.request, urllib.error
import traceback
import pandas as pd
import sys
from threading import *
from time import sleep

def test_web_error(url):
    '''
    Given a url, this function tests 404, 200 error.
    Return 0: broken link
    Return 1: link works
    '''
    try:
        conn = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        return 0
    except urllib.error.URLError as e:
        return 0
    else:
        return 1

def test_html_error(item_folder_name, oclc_number):
    '''
    This functions tests if the RDF file has HTML error.
    If no: return None
    If yes: return the OCLC number
    '''
    with open(item_folder_name + '/' + oclc_number + '.rdf', 'r', encoding="utf-8") as f:
        data = f.read()
    if data[:6] == "<html>":
        return oclc_number
    else:
        return None

def read_oclc_number_txt(filename):
    '''
    This function reads a .txt (oclc number), returns a list of 
    strings of OCLC numbers
    '''
    with open(filename) as f:
        content = f.readlines()
    # remove whitespace characters like `\n` at the end of each line
    content = [x.strip() for x in content] 
    return content


def get_oclc_records(filename):
    '''
    This function reads a .txt file and returns a dictionary with key as the 
    OCLC number and values as a dictionary. 

    The output is the input of 
    chunk_oclc_dictionary(dic, SIZE=6).
    '''
    oclc_number_list = read_oclc_number_txt(filename)
    d = {}
    for i in oclc_number_list:
        d[i] = {}
    return d

def chunk_oclc_dictionary(dic, SIZE=6):
    '''
    This function splits the dictionary we get from get_oclc_records into
    a list of 6 dictionaries (6 threads)
    '''
    i = itertools.cycle(range(SIZE))
    split = [dict() for _ in range(SIZE)]
    for k, v in dic.items():
        split[next(i)][k] = v
    return split

def download_rdf_per_dic(dic, item_folder_name, all_item_names):
    '''
    Download the rdf of all oclc numbers in the input dictionary
    with timeout function

    Reference: https://stackoverflow.com/questions/32763720/timeout-a-file
    -download-with-python-urllib
    '''

    initial_download_url = "http://www.worldcat.org/oclc/"
    for key in dic:
    	if key not in all_item_names:
            source_url = initial_download_url + key + ".rdf"
            local_path = item_folder_name + "/" +key + ".rdf"
            # Make the actual request, set the timeout for no data to 10 seconds 
            # and enable streaming responses so we don't have to keep the large 
            # files in memory
            request = requests.get(source_url, timeout=10, stream=True)
            # Open the output file and make sure we write in binary mode
            with open(local_path, 'wb') as fh:
                # Walk through the request response in chunks of 1024 * 1024 
                # bytes, so 1MiB
                for chunk in request.iter_content(1024 * 1024):
                    # Write the chunk to the file
                    fh.write(chunk)
            all_item_names.append(key)
            sleep(1)


def download_all_rdf(l, item_folder_name):
    '''
    Given a list of all oclc numbers, this function download it dictionary 
    by dictionary and save the files in the folder user provided.
    '''
    pattern = glob.glob(item_folder_name + "/*.rdf")
    all_item_names = [i.lstrip("RDF\\").rstrip(".rdf") for i in pattern]
    thread_pool = []
    for i in range(len(l)):
        thread_pool.append(Thread(target=download_rdf_per_dic, args=(l[i],item_folder_name, all_item_names, )))
    for i in thread_pool:
        i.start()
    for i in thread_pool:
        i.join()

def parse_one_rdf(oclc_number, all_subs, item_folder_name, sub_folder_name,
                  c_report, s_report):
    '''
    Given an OCLC number (string), this function return its url of
    creator/contributor/subject LCSH, if any.
    Input:
        oclc_number: a string of digits
        all_subs: the names of all subject .rdf files we have already
                  downloaded
        sub_folder_name: the address of the folder storing subject .rdf
        item_folder_name: the address of the folder storing item.rdf
        c_report: boolean, save report of creator/contributor or not
        s_report: boolean, save report of subject or not
    output:
        dic: a dictionary, key is oclc number, value is a dictionary, which has
           key as the one of the value 'creator'/ 'contributor'/'subject'
           and value as either ['type', linked data of creator/contributor] or
           a list of subjects
        one_sub_report:a report of subject that will later be written as a
                       .txt later, a list of strings
        one_cre_report:a report of creator that will later be written as a
                       .txt later, a list of strings
        one_con_report:a report of contributor that will later be written as a
                       .txt later, a list of strings
    '''
    dic = {}
    graph = rdflib.Graph()
    graph.open("store", create=True)
    graph.parse(item_folder_name + "/" + oclc_number + '.rdf')
    record_url = "http://www.worldcat.org/oclc/" + oclc_number

    # get_creator
    cre_qres = graph.query(
        """SELECT ?creator
           WHERE {
              ?record_url schema:creator  ?creator .
           }""")
    if cre_qres:
        dic, one_cre_report = c_dictionary_get(oclc_number, cre_qres,
                                               "creator", dic, c_report)
    else:  # if c_report == False
        one_cre_report = []

    # get_contributor
    con_qres = graph.query(
        """SELECT ?contributor
           WHERE {
              ?record_url schema:contributor  ?contributor .
           }""")
    if con_qres:
        dic, one_con_report = c_dictionary_get(oclc_number, con_qres,
                                               "contributor", dic, c_report)
    else:
        one_con_report = []

    # get_subject
    sub_qres = graph.query(
        """SELECT ?sub
           WHERE {
              ?record_url schema:about  ?sub .
           }""")
    if sub_qres:
        dic, all_subs, one_sub_report = s_dictionary_get(sub_qres,
                                                         oclc_number, dic, all_subs, sub_folder_name, s_report)
    else:
        one_sub_report = []

    return dic, all_subs, one_sub_report, one_cre_report, one_con_report

def c_dictionary_get(oclc_number, query_result, key, dic, c_report):
    '''
    Get the VIAF link and type of the creator/contributor.
    Input:
        oclc number: a string of digits
        query_result: the SPARQL query result from the function, parse_one_rdf
        key: 'creator' or 'contributor'
        dic: a dictionary, key is oclc number, value is a dictionary, which has
           key as the one of the value 'creator'/ 'contributor'/'subject'
           and value as either ['type', linked data of creator/contributor] or
           a list of subjects
        c_report: boolean, save report of creator/contributor or not
    Output:
        dic:a dictionary, key is oclc number, value is a dictionary, which has
           key as the one of the value 'creator'/ 'contributor'/'subject'
           and value as either ['type', linked data of creator/contributor] or
           a list of subjects
        report: a report that will later be written as a .txt later, a list of
                strings
    '''
    report = []
    if c_report:  # for report
        report.append("****************************************************")
        l1 = "For OCLC number " + oclc_number + " :"
        report.append(l1)

    for row, in query_result:
        link = str(row)
        # talked with MJ on 3/5, good enough linked data
        if 'http://experiment.worldcat.org' in link:
            if c_report:  # for report
                report.append("Experiment in the URL, pass")

        else:
            if test_web_error(link) == 0:  # link not works
                if c_report:  # for report
                    l2 = 'The link is invalid ' + link
                    report.append(l2)
                pass
            else:
                if c_report:  # for report
                    l3 = 'Start tracking the following subject URLs in linked data ' + link
                    report.append(l3)

                graph = rdflib.Graph()
                graph.open("store", create=True)
                graph.parse('RDF/' + oclc_number + '.rdf')
                # Build and execute the query
                q_pre = sparql.prepareQuery((
                    """SELECT ?value
                       WHERE {
                          ?viaf rdf:type ?value .
                       }"""))
                viaf_url = rdflib.URIRef(str(row))
                qres_type = graph.query(q_pre, initBindings={'viaf': viaf_url})

                for my_type, in qres_type:
                    viaf_type = str(my_type)

                if c_report:  # for report
                    l4 = 'Type of the link, ' + link + ' is ' + viaf_type
                    report.append(l4)
                if key not in dic:
                    dic[key] = [[link, viaf_type]]
                else:
                    dic[key].append([link, viaf_type])
    return dic, report

def s_dictionary_get(query_result, oclc_number, dic, all_subs, sub_folder_name,
                     s_report):
    '''
    Get the subject linked data, including LCSHs, geonames, and LC name
    authorities. Because we need to download FAST.rdf to query the LCSHs,
    we use all_subs to keep track of the name of FAST files we have downloaded
    Input:
        query_result: the SPARQL query result from the function, parse_one_rdf
        oclc number: a string of digits
        key: 'creator' or 'contributor'
        dic: a dictionary, key is oclc number, value is a dictionary, which has
           key as the one of the value 'creator'/ 'contributor'/'subject'
           and value as either ['type', linked data of creator/contributor] or
           a list of subjects
        all_subs: a list of all the names of downloaded subject.rdf
        sub_folder_name: the address of the folder storing subject .rdf
        s_report: boolean, save report of subject or not
    Output:
        dic:a dictionary, key is oclc number, value is a dictionary, which has
           key as the one of the value 'creator'/ 'contributor'/'subject'
           and value as either ['type', linked data of creator/contributor] or
           a list of subjects
        all_subs: a list of all the names of downloaded subject.rdf
        report: a report that will later be written as a .txt later, a list of
                strings
    '''
    r_lst = []  # a list of all subject linked data for one OCLC
    report = []  # a list of strings

    if s_report:  # for report
        report.append("*****************************************************")
        l0 = "For OCLC number " + oclc_number + " :"
        report.append(l0)

    for link, in query_result:
        sub_ori = str(link)
        if s_report:
            report.append("")
            l2 = "Start tracking the following subject URLs " + str(link)
            report.append(l2)

        if 'http://experiment.worldcat.org' in sub_ori or 'http://dewey.info' in sub_ori:
            if s_report:  # for report
                l8 = "experiment or dewey is in the linked data " + sub_ori + " , PASS"
                report.append(l8)
            pass

        else:
            if test_web_error(sub_ori) == 0:  # test if link is valid
                if s_report:
                    l1 = 'The URL is broken, PASS'
                    report.append(l1)
                pass

            else:
                # we don't download work id files
                if "http://www.worldcat.org/oclc/" in sub_ori:
                    if s_report:  # for report
                        report.append("www.worldcat.org/oclc/ found in the link, pass")

                elif "http://id.loc.gov/authorities/subjects" in sub_ori:
                    if sub_ori not in r_lst:
                        r_lst.append(sub_ori)

                        if s_report:  # for report
                            l4 = "LCSH found the linked data, new LCSH " + sub_ori + " , ADDED"
                            report.append(l4)

                    else:
                        if s_report:  # for report
                            l5 = "LCSH found the linked data, old LCSH " + sub_ori + " ,PASS"
                            report.append(l5)
                            report.append(" ")
                        pass

                elif "http://viaf.org/viaf/" in sub_ori:
                    if sub_ori not in r_lst:
                        r_lst.append(sub_ori)

                        if s_report:  # for report
                            l6 = "VIAF found the linked data " + sub_ori + " ,ADDED"
                            report.append(l6)
                            report.append("")
                            # FAST API
                elif "http://id.worldcat.org/fast/" in sub_ori:
                    lcsh_num = sub_ori.split("/")[-1]
                    local_path = sub_folder_name + "/" + lcsh_num + '.rdf'

                    if lcsh_num not in all_subs:
                        test_query = sub_ori + '/rdf.xml'
                        request = requests.get(test_query, timeout=50, stream=True)
                        with open(local_path, 'wb') as fh:
                            # Walk through the request response in chunks of
                            # 1024 * 1024 bytes, so 1MiB
                            for chunk in request.iter_content(1024 * 1024):
                                # Write the chunk to the file
                                fh.write(chunk)
                        all_subs.append(lcsh_num)

                    # parse the already downloaded rdf
                    graph = rdflib.Graph()
                    graph.open("store", create=True)
                    graph.parse(local_path)

                    # get_subject
                    qres = graph.query(
                        """SELECT ?object
                        WHERE {
                          ?sub_ori schema:sameAs ?object .
                        }""")

                    for row, in qres:
                        if str(row) not in r_lst:
                            r_lst.append(str(row))

                            if s_report:  # for report
                                l7 = "get LCSH " + str(row)
                                report.append(l7)
                                report.append(" ")

                else:
                    if sub_ori not in r_lst:
                        r_lst.append(sub_ori)

                        if s_report:  # for report
                            l10 = "Link is odd, new case,  " + sub_ori
                            report.append(l10)
                            report.append(" ")
    if len(r_lst) > 0:
        dic['subject'] = r_lst
    return dic, all_subs, report

def parse_rdf_all_per_d(dic, item_folder_name, sub_folder_name,
                        c_report=False, s_report=False):
    '''
    Get the subject linked data, including LCSHs, geonames, and LC name
    authorities. Because we need to download FAST.rdf to query the LCSHs,
    we use all_subs to keep track of the name of FAST files we have downloaded
    Input:
        dic: a dictionary of oclc numbers (key), empty list as values
        item_folder_name: the address of the folder storing item.rdf
        sub_folder_name: the address of the folder storing subject .rdf
        c_report: boolean, save report of creator/contributor or not
        s_report: boolean, save report of subject or not
    Output:
        new_d: a dictionary of all oclc_numbers with linked data
               key: oclc number, a string of digits
               value: a dictionary
                      {'creator': [[type, link],...],
                       'contributor':[[type, link],...],
                        'subject': [link, link,link,....]}
        error_list: a list of oclc_number that fails to download or process
    '''
    error_list = []
    new_d = {}  # dictionary of all oclc_numbers with linked data

    all_subs = []
    with_prefix_sufix_sub = glob.glob("Sub/*.rdf")
    all_subs = [i.lstrip("Sub\\").rstrip(".rdf") for i in
                with_prefix_sufix_sub]

    sub_report = []
    cre_report = []
    con_report = []

    for oclc_number in dic:
        print(oclc_number)

        if test_html_error(item_folder_name, oclc_number):
            error_list.append(oclc_number)
        else:
            d, all_subs, one_sub_report, one_cre_report, one_con_report = parse_one_rdf(oclc_number, all_subs,
                                                                                        item_folder_name,
                                                                                        sub_folder_name, c_report,
                                                                                        s_report)

            new_d[oclc_number] = d

            if s_report:
                sub_report += one_sub_report
            if c_report:
                cre_report += one_cre_report
                con_report += one_con_report

    if s_report:
        with open(r'Report/subject_report.txt', 'w') as f_sub:
            for i in sub_report:
                f_sub.write(i + "\n")

    if c_report:
        with open(r'Report/creator_report.txt', 'w') as f_cre:
            for i in cre_report:
                f_cre.write(i + "\n")

    if c_report:
        with open(r'Report/contributoor_report.txt', 'w') as f_con:
            for i in con_report:
                f_con.write(i + "\n")

    return new_d, error_list

def get(oclc_txt_folder_name, item_folder_name, sub_folder_name, SIZE, c_report=False, s_report=False):
    '''
    Download all OCLC.rdf files that are in the oclc_txt_folder_name and get a csv and a .txt that the program
    fails to download.
    Each row is one OCLC record, column are the linked data for that record.
    '''
    d = get_oclc_records(oclc_txt_folder_name)
    l = chunk_oclc_dictionary(d, SIZE)
    download_all_rdf(l, "RDF")

    new_d, error_list = parse_rdf_all_per_d(d, item_folder_name, sub_folder_name, c_report, s_report)

    df = pd.DataFrame(new_d).T
    df.to_csv('Result.csv')

    with open("errors.txt", 'w') as f:
        f.write("\n".join(map(str, error_list)))

if __name__ == "__main__":
    num_args = len(sys.argv)
    if sys.argv[-2] == 'T':
        c_report = True
    else:
        c_report = False
    if sys.argv[-1] == 'T':
        s_report = True
    else:
        s_report = False

    if num_args == 7:
        get(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], c_report, s_report)
    elif num_args == 6:
        get(sys.argv[1], sys.argv[2], sys.argv[3], 6, c_report, s_report)
