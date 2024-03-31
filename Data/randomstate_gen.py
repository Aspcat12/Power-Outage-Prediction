import multiprocessing
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import time
import os

# init variable

start = time.time()
EXCEL = pd.read_excel('excel_convert.xlsx',sheet_name='Sheet2 (2)')
rf_classifier = RandomForestClassifier(random_state=42)
dummy = EXCEL.copy()
X = dummy[['tempC','sunHour','pressure','DewPointC','FeelsLikeC',
           'HeatIndexC','WindChillC','WindGustKmph','cloudcover','humidity','precipMM',
           'winddirDegree','windspeedKmph','OpDeviceType','maxtempC','mintempC','moon_illumination','Province',
           'Water_pixels','Tree_pixels','Grass_pixels','Flooded_vegetation_pixels','Crops_pixels',
           'Shrub_and_Scrub_pixels','Built_pixels','Bare_pixels','Snow_and_Ice_pixels']]
Y = dummy['CauseType']

def task(row):
    # กำหนดตัวแปรใหม่ทุกครั้งที่ใช้ function เพื่อป้องกันไม่ให้ตัวแปรข้อมูลหายตอนรัน multiprocessing
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=row)
    rf_classifier.fit(X_train,Y_train)
    rf_score = rf_classifier.score(X_test,Y_test)
    print(f"rf_score = {rf_score} ---- random_state = {row} is finished at {(time.time()-start):.2f} Seconds", flush=True)
    return rf_score,row
        
def main():

    # task to do
    print('Starting now')
    EXCEL_Results = pd.read_excel('score.xlsx',sheet_name='Sheet1')
    pool = multiprocessing.Pool(processes=os.cpu_count())

    for score,row in pool.imap_unordered(task, range(1000)):
        EXCEL_Results.loc[row, 'score'] = score
        EXCEL_Results.loc[row, 'random_state'] = row

    # wait for tasks to finish:
    pool.close()
    pool.join()

    # Get and Export Results
    print(EXCEL_Results)
    EXCEL_Results.to_excel('score_done2.xlsx', index=False)
    print(f'ALL TASK IS FINISHED AT {time.time()-start} Seconds')


# required for Windows:
if __name__ == '__main__':
    # Starting task
    main()

