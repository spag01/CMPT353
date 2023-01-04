from turtle import pos
import pandas as pd
from scipy import stats
from statsmodels.stats.multicomp import pairwise_tukeyhsd

def get_specific_sorting_column(data,s):
    return data[s]

def qs1(data):
    return get_specific_sorting_column(data,'qs1')

def qs2(data):
    return get_specific_sorting_column(data, 'qs2')

def qs3(data):
    return get_specific_sorting_column(data,'qs3')

def qs4(data):
    return get_specific_sorting_column(data,'qs4')

def qs5(data):
    return get_specific_sorting_column(data,'qs5')

def merge1(data):
    return get_specific_sorting_column(data,'merge1')

def partition_sort_data(data):
    return get_specific_sorting_column(data,'partition_sort')

def print_anova_p_value(data):
    anova = stats.f_oneway(qs1(data), qs2(data), qs3(data), qs4(data), qs5(data), merge1(data), partition_sort_data(data))
    print(anova.pvalue)

def plot_figure(data):
    plt = data.plot_simultaneous()
    plt.savefig('result_plot.png')

def hoc_anaylsis(data):
    melt_data= pd.melt(data)
    post_data = pairwise_tukeyhsd(melt_data['value'], melt_data['variable'],alpha=0.05)
    print(post_data)
    plot_figure(post_data)

data = pd.read_csv('data.csv')

# this value signifies that each algorithm has it's own time complexity which is not same as others
print_anova_p_value(data)

# post anaylsis
hoc_anaylsis(data)
