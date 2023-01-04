from operator import ge
import sys
from xxlimited import new
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import mannwhitneyu, chi2_contingency


OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value:  {more_users_p:.3g}\n'
    '"Did users search more/less?" p-value:  {more_searches_p:.3g} \n'
    '"Did more/less instructors use the search feature?" p-value:  {more_instr_p:.3g}\n'
    '"Did instructors search more/less?" p-value:  {more_instr_searches_p:.3g}'
)


def get_new_searches(d):
    return d[d['uid']%2==1]

def get_original_search(d):
    return d[d['uid']%2==0]

def get_instr(d):
    return d[d['is_instructor']==True]

def get_p_searches(new_searches, original_searches):
    new_atleast_once = new_searches[new_searches['search_count']>0]['uid'].count()
    original_atleast_once = original_searches[original_searches['search_count']>0]['uid'].count()
    new_never = new_searches[new_searches['search_count']==0]['uid'].count()
    original_never = original_searches[original_searches['search_count']==0]['uid'].count()

    contigency_searchdata = [[new_atleast_once, new_never],[original_atleast_once, original_never]]
    chi2_searches, p_searches, dof_searches, ex_searches  = chi2_contingency(contigency_searchdata)
    return p_searches

def get_p_users(new_searches, original_searches):
    return mannwhitneyu(new_searches['login_count'], original_searches['login_count']).pvalue

def get_p_instr_search(new_instructors, original_instructors):
    new_atleast_once_instructors = new_instructors[new_instructors['search_count']>0]['uid'].count()
    original_atleast_once_instructors = original_instructors[original_instructors['search_count']>0]['uid'].count()
    new_never_instructors = new_instructors[new_instructors['search_count']==0]['uid'].count()
    original_never_instructors = original_instructors[original_instructors['search_count']==0]['uid'].count()

    instructors_contigency = [[new_atleast_once_instructors, new_never_instructors],[original_atleast_once_instructors, original_never_instructors]]
    chi2_instructors, p_instr_searches, dof_instructors, ex_instructors  = chi2_contingency(instructors_contigency)
    return p_instr_searches    

def main():
    searchdata_file = sys.argv[1]
    searchdata = pd.read_json(searchdata_file, orient='records', lines=True)

    # Seperate new and original search data
    new_searches = get_new_searches(searchdata)
    original_searches = get_original_search(searchdata)

    # For Searches
    p_users = get_p_users(new_searches,original_searches)
    p_searches = get_p_searches(new_searches, original_searches)

    # Seperate new and original instructor data
    new_instructors = get_instr(new_searches)
    original_instructors = get_instr(original_searches)

    # For Instructors
    p_instr_searches = get_p_instr_search(new_instructors,original_instructors)
    p_instr = mannwhitneyu(new_instructors['login_count'], original_instructors['login_count']).pvalue

    # Output
    print(OUTPUT_TEMPLATE.format(
        more_users_p= p_users,
        more_searches_p= p_searches,
        more_instr_p= p_instr,
        more_instr_searches_p= p_instr_searches,
    ))


if __name__ == '__main__':
    main()