import openpyxl
import pyarrow
import fastparquet
import pandas as pd

# function which gives the stack replacement or total capex for an electrolyser for a given size in kW
def interp(interp_dict, technology, size_kW):
    '''
    Input: 
    interp_dict: the interpolated dictionary you want to pull values from 
    technology: the electrolyser you want the data for 
    size_kW: the size of electrolyser you want the data for
    
    Output: the value from the inputted dictionary for the given size for given electrolyser
    '''
    return float(interp_dict[technology](size_kW))

def read_data():
    '''
    Outputs:
    
    
    '''

    df_load = pd.read_parquet("/Users/eishitakalra/Desktop/Dissertation!!/Data/DissertationWoodMackenzieHydrogen/hourly_production_prices.parquet", engine='fastparquet')

    df_RE = df_load[["Report_Year","Report_Month","Report_Day","Report_Hour","Solar (MWh)","Wind Onshore (MWh)", "Wind Offshore (MWh)", "Price (EUR/MWh, real 2025)"]].copy()
    df_RE.loc[:, "Solar (kWh)"] = df_RE["Solar (MWh)"] * 1000
    df_RE.loc[:, "Wind Offshore (kWh)"] = df_RE["Wind Offshore (MWh)"] * 1000
    df_RE.loc[:, "Wind Onshore (kWh)"] = df_RE["Wind Onshore (MWh)"] * 1000
    df_RE.loc[:, "Price (EUR/kWh, real 2025)"] = df_RE["Price (EUR/MWh, real 2025)"] * 1000

    df_Grid = pd.read_excel('model_hourly_intensities_2025_H1_v1.xlsx',sheet_name='Hourly Data',skiprows=4,usecols="B:F")
    df_Grid.rename(columns={'Great Britain':'CO2 Intensity (g CO2/kWh)'},inplace=True)
    # Putting Price of grid and Intensity of Grid into one df
    df_Grid = df_Grid.merge(
        df_RE[['Report_Year', 'Report_Month', 'Report_Day', 'Report_Hour', 'Price (EUR/kWh, real 2025)']],
        on=['Report_Year', 'Report_Month', 'Report_Day', 'Report_Hour'],
        how='left'
    )

    # Delete data in MW/MWh and Price data from df_RE, only keep renewable data
    df_RE = df_RE.drop(columns=['Price (EUR/kWh, real 2025)', 'Price (EUR/MWh, real 2025)','Solar (MWh)', 'Wind Onshore (MWh)','Wind Offshore (MWh)'])



    df_Elctro_Costs = pd.DataFrame({
        'Technology': ['PEM','PEM','PEM','ALK','ALK','ALK','SOEC','SOEC','SOEC'],
        'Scale (kW)': [2000,20000,200000,2000,20000,200000,2000,20000,200000],
        'Balance of Stack ($/kW)': [867.0520341,846.7366522, 790.7136808, 672.5614143, 651.3272692, 597.4636084, 1590.532075, 1561.510838, 1503.745575],
        'Stack cappex': [578.0346894, 564.4911015, 527.1424539, 448.3742762, 434.2181795, 398.3090723, 1060.354717, 1041.007225, 1002.49705],
        'Balance of Plant ($/kW)': [ 237 , 209 ,158 , 468 , 414 , 313 , 595 , 467 , 341 ],
        'Engineering, Procurement & Construction costs ($/kW)': [951, 1169, 1547, 923, 1468, 1987, 703, 1294, 2182],
        'Owners costs ($/kW)': [235, 327, 423, 293, 456, 599, 184, 357, 577],
        'Total Installed Cost (TIC) ($/kW)': [2868, 3117, 3447, 2805, 3424, 3895, 4133, 4720, 5606],
    })

    df_Elctro = pd.DataFrame({
        'Type': ['PEM', 'ALK', 'SOEC'],
        'kWh/kg H2': [55.956916, 55.479739, 42.881849],
        'Minimum Load': [0.05, 0.1, 0.05],
        'Maximum Load': [ 1, 1, 1],
        'Stack Lifetime (hours)': [ 60000, 100000, 60000],
        'Efficiency degradation / year': [0.005, 0.0075, 0.005],
        'Fixed Opex percent': [0.03,0.03,0.03],
    })

    df_Battery = pd.DataFrame({
        'Type': ['Lithium Batteries'],
        'Capex ($/kWh)': [300],
        'Fixed Opex percent': [0.025],
        'Round trip efficiency': [0.85],
        'Duration (hrs)': [4],
    })

    # df_H2Store = pd.DataFrame({
    #     'Type':
    #     'Capex':
    #     'Opex':
    #     'Efficiency':
    # })

    df_PPA = pd.DataFrame({
        'Renewable Source': ['Solar PV', 'Wind Onshore', 'Wind Offshore'],
        'PPA Price (£/kWh)': [ 0.049, 0.0456, 0.0422],
    })

    df_repperiods = pd.DataFrame({
        'K': ['January','February','March','April','May','June','July','August','September','October','November','December'],
        'TB': [0,744,1416,2160,2880,3624,4344,5088,5832,6552,7296,8016],
        'TE': [71,815,1487,2231,2951,3695,4415,5159,5903,6623,7367,8087],
        'weights': [1/12,1/12,1/12,1/12,1/12,1/12,1/12,1/12,1/12,1/12,1/12,1/12],
    })

    return df_RE, df_Grid, df_Elctro, df_Elctro_Costs, df_Battery, df_PPA, df_repperiods


# def find_rep_periods(n,year_start,year_end,month_start,month_end,day_start,day_end, freq='H',weights=None):