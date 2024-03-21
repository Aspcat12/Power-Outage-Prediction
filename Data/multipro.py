import multiprocessing
import ee
from ee_jupyter.ipyleaflet import Map
from ee_jupyter.ipyleaflet import Inspector
from ee_jupyter.layout import MapWithInspector
import ipyleaflet
import pandas as pd
import time
import os

# init variable

# Specify the path to your service account key file (JSON)
key_path = 'C:/Users/Kittipos/Desktop/py/ee-kittipos6199-d97d93f5bb4e.json'
# Create ServiceAccountCredentials
credentials = ee.ServiceAccountCredentials(key_file=key_path,email='kittipos6199@gmail.com')
# Authenticate using the credentials
ee.Initialize(credentials)

start = time.time()
EXCEL = pd.read_excel('event occurred(filtered).xlsx',sheet_name='Sheet5')

def task(row):
    # กำหนดตัวแปรใหม่ทุกครั้งที่ใช้ function เพื่อป้องกันไม่ให้ตัวแปรข้อมูลหายตอนรัน multiprocessing
    Max_Try = 3
    Try = 0
    EXCEL_TO_USE = EXCEL.copy()
    while Try < Max_Try:
        try:
            dw_base = ee.ImageCollection('GOOGLE/DYNAMICWORLD/V1')

            dw_classes = {
                '0':{'LULC Type': 'Water',
                    'band name': 'water',
                    'color': '#419BDF',
                    'description': 'Permanent and seasonal water bodies'},
                '1':{'LULC Type': 'Trees',
                    'band name': 'trees',
                    'color': '#397D49',
                    'description': 'Includes primary and secondary forests, as well as large-scale plantations'},
                '2':{'LULC Type': 'Grass',
                    'band name': 'grass',
                    'color': '#88B053',
                    'description': 'Natural grasslands, livestock pastures, and parks'},
                '3':{'LULC Type': 'Flooded vegetation',
                    'band name': 'flooded_vegetation',
                    'color': '#7A87C6',
                    'description': 'Mangroves and other inundated ecosystems'},
                '4':{'LULC Type': 'Crops',
                    'band name': 'crops',
                    'color': '#E49635',
                    'description': 'Include row crops and paddy crops'},
                '5':{'LULC Type': 'Shrub & Scrub',
                    'band name': 'shrub_and_scrub',
                    'color': '#DFC35A',
                    'description': 'Sparse to dense open vegetation consisting of shrubs'},
                '6':{'LULC Type': 'Built Area',
                    'band name': 'built',
                    'color': '#C4281B',
                    'description': 'Low- and high-density buildings, roads, and urban open space'},
                '7':{'LULC Type': 'Bare ground',
                    'band name': 'bare',
                    'color': '#A59B8F',
                    'description': 'Deserts and exposed rock'},
                '8':{'LULC Type': 'Snow & Ice',
                    'band name': 'snow_and_ice',
                    'color': '#B39FE1',
                    'description': 'Permanent and seasonal snow cover'},
            }

            probability_bands = [
            'water', 'trees', 'grass', 'flooded_vegetation', 'crops', 'shrub_and_scrub',
            'built', 'bare', 'snow_and_ice'
            ]

            geometry = ee.Geometry.Rectangle(EXCEL_TO_USE['LONG'][row]-0.005, EXCEL_TO_USE['LAT'][row]-0.005, EXCEL_TO_USE['LONG'][row]+0.005, EXCEL_TO_USE['LAT'][row]+0.005)
            start_date = EXCEL_TO_USE['1 year before'][row]
            end_date = EXCEL_TO_USE['Event Date'][row]
            dw = (dw_base.filterDate(start_date, end_date)
                        .filterBounds(geometry))

            probability_col = dw.select(probability_bands)

            # Create an image with the average pixel-wise probability
            # of each class across the time-period.
            mean_probability = probability_col.reduce(ee.Reducer.mean())

            dw_class_composite = mean_probability.toArray().arrayArgmax().arrayGet(0).rename('label')

            reduction_scale = 10

            pixel_count_stats = dw_class_composite.reduceRegion(
                reducer=ee.Reducer.frequencyHistogram().unweighted(),
                geometry=geometry,
                scale=reduction_scale,
                maxPixels=1e10
                )

            # Return Results in variable

            # pixel_counts = ee.Dictionary(pixel_count_stats.get('label'))
            # try:
            #     Water_pixel = pixel_counts.getInfo()['0']
            # except:
            #     Water_pixel = 0
            # try:
            #     tree_pixel = pixel_counts.getInfo()['1']
            # except:
            #     tree_pixel = 0
            # try:
            #     grass_pixel = pixel_counts.getInfo()['2']
            # except:
            #     grass_pixel = 0
            # try:
            #     flooded_vegetation_pixel = pixel_counts.getInfo()['3']
            # except:
            #     flooded_vegetation_pixel = 0
            # try:
            #     crops_pixel = pixel_counts.getInfo()['4']
            # except:
            #     crops_pixel = 0
            # try:
            #     shrub_and_scrub_pixel = pixel_counts.getInfo()['5']
            # except:
            #     shrub_and_scrub_pixel = 0
            # try:
            #     built_pixel = pixel_counts.getInfo()['6']
            # except:
            #     built_pixel = 0
            # try:
            #     bare_pixel = pixel_counts.getInfo()['7']
            # except:
            #     bare_pixel = 0
            # try:
            #     snow_and_ice_pixel = pixel_counts.getInfo()['8']
            # except:
            #     snow_and_ice_pixel = 0

            # Create a dictionary to store pixel values dynamically
            pixel_values = {}
            pixel_counts = ee.Dictionary(pixel_count_stats.get('label'))
            for label in range(9):  # Assuming labels are 0 to 8
                key = str(label)
                pixel_count = pixel_counts.getInfo().get(key, 0)
                # Save pixel values to the dictionary using the label name
                pixel_values[dw_classes[key]["LULC Type"]] = pixel_count  

            break #break out of while loop

        except Exception as e:
            print(f"Error in task for row {row}. Retrying #{Try}",type(e),flush=True)
            time.sleep(1)  # Add a small delay before retrying
            Try += 1

    print(f'Task #{row} is finished at {time.time()-start} Seconds', flush=True)
    del EXCEL_TO_USE,dw_base,dw_classes,dw_class_composite,pixel_count_stats
    return (
        pixel_values['Water'], pixel_values['Trees'], pixel_values['Grass'],
        pixel_values['Flooded vegetation'], pixel_values['Crops'],
        pixel_values['Shrub & Scrub'], pixel_values['Built Area'],
        pixel_values['Bare ground'], pixel_values['Snow & Ice'], row
    )
            # return Water_pixel,tree_pixel,grass_pixel,flooded_vegetation_pixel,crops_pixel,shrub_and_scrub_pixel,built_pixel,bare_pixel,snow_and_ice_pixel,row

        
def main():

    # task to do
    print('Starting now')
    EXCEL_Results = EXCEL.copy()
    pool = multiprocessing.Pool(processes=os.cpu_count())

    for Water,tree,grass,flooded_vegetation,crops,shrub_and_scrub,built,bare,snow_and_ice,row in pool.imap_unordered(task, range(len(EXCEL_Results))):
        EXCEL_Results.loc[row, 'Water_pixels'] = Water
        EXCEL_Results.loc[row, 'Tree_pixels'] = tree
        EXCEL_Results.loc[row, 'Grass_pixels'] = grass
        EXCEL_Results.loc[row, 'Flooded_vegetation_pixels'] = flooded_vegetation
        EXCEL_Results.loc[row, 'Crops_pixels'] = crops
        EXCEL_Results.loc[row, 'Shrub_and_Scrub_pixels'] = shrub_and_scrub
        EXCEL_Results.loc[row, 'Built_pixels'] = built
        EXCEL_Results.loc[row, 'Bare_pixels'] = bare
        EXCEL_Results.loc[row, 'Snow_and_Ice_pixels'] = snow_and_ice

    # wait for tasks to finish:
    pool.close()
    pool.join()

    # Get and Export Results
    print(EXCEL_Results)
    EXCEL_Results.to_excel('All-pixel_1-year-before.xlsx', index=False)
    print(f'ALL TASK IS FINISHED AT {time.time()-start} Seconds')


# required for Windows:
if __name__ == '__main__':
    # Starting task
    main()

