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
    df_RE.loc[:, "Solar PV"] = df_RE["Solar (MWh)"] * 1000
    df_RE.loc[:, "Wind Offshore"] = df_RE["Wind Offshore (MWh)"] * 1000
    df_RE.loc[:, "Wind Onshore"] = df_RE["Wind Onshore (MWh)"] * 1000
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

    # Change Grid data: EUR/kWh to £/kWh and g CO2 /kg H2 to kg CO2/ kg H2:
    df_Grid['Price (£/kWh, real 2025)'] = df_Grid['Price (EUR/kWh, real 2025)']*0.854987068320592
    df_Grid = df_Grid.drop(columns=['Price (EUR/kWh, real 2025)'])
    df_Grid['CO2 Intensity (kg CO2/kWh)'] = df_Grid['CO2 Intensity (g CO2/kWh)']/1000
    df_Grid = df_Grid.drop(columns=['CO2 Intensity (g CO2/kWh)'])


    df_Elctro_Costs = pd.DataFrame({
        'Technology': ['PEMWE','PEMWE','PEMWE','AWE','AWE','AWE','SOEC','SOEC','SOEC'],
        'Scale (kW)': [2000,20000,200000,2000,20000,200000,2000,20000,200000],
        'Balance of Stack (£/kW)': [ 1156.603885, 1129.504185,1054.772354, 897.1631619, 868.8378783, 796.9864589, 2121.689938, 2082.977065, 2005.921104 ],
        # need to get rid of stack capex cos its not acc how it'll be calculated
        'Stack cappex': [578.0346894, 564.4911015, 527.1424539, 448.3742762, 434.2181795, 398.3090723, 1060.354717, 1041.007225, 1002.49705],
        'Balance of Plant (£/kW)': [189.7077469, 167.6759508, 126.8434368, 374.7114968, 331.1942054, 250.5416611, 476.1794118, 374.1138646, 272.9020255],
        'Engineering, Procurement & Construction costs (£/kW)': [ 760.7882026, 935.9779316, 1238.529618, 738.3592001, 1175.006147, 1590.215546, 562.7217168, 1035.348368, 1746.311902],
        'Owners costs (£/kW)': [188.0791382, 261.8253963, 338.9005951, 234.7917669, 365.1771344, 479.401487, 147.1290253, 285.3430374, 461.6929363],
        'Total Installed Cost (TIC) (£/kW)': [2295.178972, 2494.983464, 2759.046004, 2245.025626, 2740.215365, 3117.145153, 3307.720092, 3777.782335, 4486.827967],
    })

    df_Elctro = pd.DataFrame({
        'Type': ['PEMWE', 'AWE', 'SOEC'],
        'kWh/kg H2': [55.956916, 55.479739, 42.881849],
        'Minimum Load': [0.05, 0.1, 0.05],
        'Maximum Load': [ 1, 1, 1],
        # get rid of the next two cos it is also changes with time and load
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