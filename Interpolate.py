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