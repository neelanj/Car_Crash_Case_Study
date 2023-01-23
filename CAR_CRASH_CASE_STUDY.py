"""
    Title        : Car Crash Case Study
    Description  : Case Stud of the data of vehicle accidents across US for brief amount of time
    Created By   : Neelanj Kamlesh
    Created Date : 22-Jan-2023
"""
import time
import sys
import yaml
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lower



class Case_Study():
    def Analysis_1(primary_person_df):
        """
        Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?
            
            -count the crash id if person is male and is killed  
        """
        try:
            no_of_crashes = primary_person_df.select('CRASH_ID').where((primary_person_df.PRSN_GNDR_ID=='MALE') & (primary_person_df.PRSN_INJRY_SEV_ID=='KILLED')).count()
            
            return(no_of_crashes)
            
        except Exception as e:
            print('Exception: ',e)
            
    
    def Analysis_2(units_df):
        """
        Analysis 2: How many two wheelers are booked for crashes?
            
            -count the crash id if the vehicle is 2 wheeler and is charged for crash  
        """ 
        try:
            motorcycle = ["MOTORCYCLE","POLICE MOTORCYCLE"]
            
            crashed_motoryle_df=units_df.filter(units_df.VEH_BODY_STYL_ID.isin(motorcycle))
    
            booked_count = crashed_motoryle_df.join(charges_df,(crashed_motoryle_df.CRASH_ID == charges_df.CRASH_ID) & (crashed_motoryle_df.UNIT_NBR == charges_df.UNIT_NBR) ,"inner").select(charges_df.CRASH_ID).count()
            
            return(booked_count)
            
        except Exception as e:
            print('Exception: ',e)
    
    
    def Analysis_3(primary_person_df):
        """
        Analysis 3: Which state has highest number of accidents in which females are involved?
            
            -Filter the DataFrame to only include accidents involving females
            -Group the filtered DataFrame by state and count the number of accidents
            -Sort the state counts in descending order and take the first one
        """
        try:
            #Filter the DataFrame to only include accidents involving females
            female_accidents_df=primary_person_df.filter("PRSN_GNDR_ID = 'FEMALE'")
            
            # Group the filtered DataFrame by state and count the number of accidents
            state_counts = female_accidents_df.groupBy("DRVR_LIC_STATE_ID").count()
            
            # Sort the state counts in descending order and take the first one
            highest_state = state_counts.sort("count", ascending=False).first()
            
            # Print the state with the highest number of accidents involving females
            #print("State with highest number of accidents involving females: " + highest_state["DRVR_LIC_STATE_ID"])
            return(highest_state["DRVR_LIC_STATE_ID"])
            
        except Exception as e:
            print('Exception: ',e)
            
    
    def Analysis_4(primary_person_df,units_df):
        """
        Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
            
            -Fetching only injured person data
            -Group the DataFrame by VEH_MAKE_ID and count the number of injuries including death
            -Sort the VEH_MAKE_ID counts in descending order
            -Select the top 5th to 15th VEH_MAKE_ID
        """
        try:
            #Fetching only injured person data 
            injury = ["POSSIBLE INJURY","NON-INCAPACITATING INJURY","INCAPACITATING INJURY","KILLED"]
            injury_df=primary_person_df.filter(primary_person_df.PRSN_INJRY_SEV_ID.isin(injury))
            
            #joining desired tables 
            accidents_df= injury_df.join(units_df,(injury_df.CRASH_ID == units_df.CRASH_ID) & (injury_df.UNIT_NBR == units_df.UNIT_NBR) ,"inner").select(injury_df["*"],units_df["VEH_MAKE_ID"]).distinct()
            
            
            # Group the DataFrame by VEH_MAKE_ID and count the number of injuries including death
            veh_make_counts = accidents_df.groupBy("VEH_MAKE_ID").count()
            
            # Sort the VEH_MAKE_ID counts in descending order
            veh_make_counts = veh_make_counts.sort("count", ascending=False)
            
            # Select the top 5th to 15th VEH_MAKE_ID
            top_veh_make_ids = veh_make_counts.select("VEH_MAKE_ID").rdd.flatMap(lambda x: x).collect()[4:14]
            
            # Print the top 5th to 15th VEH_MAKE_ID
            #print("Top 5th to 15th VEH MAKE IDs: " + str(top_veh_make_ids))
            return(str(top_veh_make_ids))
            
        except Exception as e:
            print('Exception: ',e)
    
    
    def Analysis_5(primary_person_df,units_df):
        """
        Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
            
            -Group the DataFrame by BodyStyle and Ethnicity, and count the number of accidents
            -Sort the DataFrame by BodyStyle and Count in descending order
            -Select the top ethnic group for each unique body style
        """
        try:
            crash_df=primary_person_df.join(units_df,(primary_person_df.CRASH_ID == units_df.CRASH_ID) & (primary_person_df.UNIT_NBR == units_df.UNIT_NBR) ,"inner").select(primary_person_df["*"],units_df["VEH_BODY_STYL_ID"]).distinct()
            
            #Group the DataFrame by BodyStyle and Ethnicity, and count the number of accidents
            ethnicity_counts = crash_df.groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").count()
            
            # Sort the DataFrame by BodyStyle and Count in descending order
            ethnicity_counts = ethnicity_counts.sort("VEH_BODY_STYL_ID","count",ascending=[True,False])
            
            # Select the top ethnic group for each unique body style
            top_ethnic_group = ethnicity_counts.select("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").dropDuplicates(["VEH_BODY_STYL_ID"])
            
            #top_ethnic_group.display()
            return(top_ethnic_group)
        
        except Exception as e:
            print('Exception: ',e)
    
    
    def Analysis_6(primary_person_df,charges_df):
        """
        Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
            
            -Filter the dataframe for crashes where alcohol is a contributing factor
            -Group the filtered DataFrame by DRVR_ZIP and count the number of crashes
            -Sort the DRVR_ZIP counts in descending order
            -Select the top 5 DRVR_ZIP Codes
        """
        try:
            driver_zip_df=primary_person_df.join(charges_df,(primary_person_df.CRASH_ID == charges_df.CRASH_ID) & (primary_person_df.UNIT_NBR == charges_df.UNIT_NBR),"inner").select(primary_person_df["*"],charges_df["CHARGE"]).distinct()
            
            #Filter the dataframe for crashes where alcohol is a contributing factor
            alcohol_crashes_df = driver_zip_df.where(lower(col('CHARGE')).like("%alcohol%"))
            
            # Group the filtered DataFrame by DRVR_ZIP and count the number of crashes
            zipcode_counts = alcohol_crashes_df.groupBy("DRVR_ZIP").count()
            
            # Sort the DRVR_ZIP counts in descending order
            zipcode_counts = zipcode_counts.sort("count", ascending=False)
            
            # Select the top 5 DRVR_ZIP Codes
            top_zipcodes = zipcode_counts.select("DRVR_ZIP").rdd.flatMap(lambda x: x).take(5)
            
            # Print the top 5 Zip Codes
            #print("Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor: " + str(top_zipcodes))
            return(str(top_zipcodes))
        
        except Exception as e:
            print('Exception: ',e)
    
    
    def Analysis_7(units_df,damages_df):
        """
        Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        
            -Filter the dataframe for accidents where no damaged property was observed and the damage level is above 4 and the car avails insurance
            -Group the filtered DataFrame by Crash ID and count the number of distinct crash IDs
        """
        try:
            no_damage_df=units_df.join(damages_df,units_df.CRASH_ID == damages_df.CRASH_ID,"inner").select(units_df["*"],damages_df["DAMAGED_PROPERTY"]).distinct()
            
            # Filter the dataframe for accidents where no damaged property was observed and the damage level is above 4 and the car avails insurance
            damage_level_above_4 = ["DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST"]
            
            no_damage_df = no_damage_df.where((lower(col('DAMAGED_PROPERTY')).like("%no damage%")) & (col('VEH_DMAG_SCL_1_ID').isin(damage_level_above_4) | col('VEH_DMAG_SCL_2_ID').isin(damage_level_above_4)) & (col('FIN_RESP_TYPE_ID').isNotNull()) )
            
            # Group the filtered DataFrame by Crash ID and count the number of distinct crash IDs
            distinct_crash_ids = no_damage_df.select("CRASH_ID").distinct().count()
            
            # Print the count of distinct crash IDs
            #print("Count of Distinct Crash IDs: " + str(distinct_crash_ids))
            return(str(distinct_crash_ids))
        
        except Exception as e:
            print('Exception: ',e)    
    
     
    def Analysis_8(endorse_df,charges_df,units_df,primary_person_df):
        """
        Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
            -Filter licensed driver having speeding charges
            -Filter for top 10 used vehicle colors
            -Filter for top 25 states with highest number of offences
            -Group the filtered DataFrame by VehicleMake and count the number of accidents
            -Sort the VehicleMake counts in descending order & Select the top 5 Vehicle Makes
        """
        try:
            speeding_df=charges_df.join(endorse_df,(charges_df.CRASH_ID == endorse_df.CRASH_ID) & (charges_df.UNIT_NBR == endorse_df.UNIT_NBR),"inner").select(charges_df["*"],endorse_df["DRVR_LIC_ENDORS_ID"]).distinct()
            
            #Filter licensed driver having speeding charges 
            drvr_lic_filter=["NONE","UNKNOWN","UNLICENSED"]
            speeding_df = speeding_df.where((lower(col('CHARGE')).like("%speed%")) & (~col('DRVR_LIC_ENDORS_ID').isin(drvr_lic_filter)))
            
            veh_clr_df=speeding_df.join(units_df,(speeding_df.CRASH_ID == units_df.CRASH_ID) & (speeding_df.UNIT_NBR == units_df.UNIT_NBR) ,"inner").select(units_df["*"],speeding_df["DRVR_LIC_ENDORS_ID"],speeding_df["CHARGE"]).distinct()
            
            # Filter for top 10 used vehicle colors
            top_colors = veh_clr_df.groupBy("VEH_COLOR_ID").count().sort("count",ascending=False).limit(10)
            top_colors_list = top_colors.select("VEH_COLOR_ID").rdd.flatMap(lambda x: x).collect()
            veh_clr_df = veh_clr_df.filter(veh_clr_df["VEH_COLOR_ID"].isin(top_colors_list))
            
            veh_states_df=primary_person_df.join(veh_clr_df,(primary_person_df.CRASH_ID == veh_clr_df.CRASH_ID) & (primary_person_df.UNIT_NBR == veh_clr_df.UNIT_NBR) ,"inner" ).select(primary_person_df["*"],veh_clr_df["DRVR_LIC_ENDORS_ID"],veh_clr_df["CHARGE"],veh_clr_df["VEH_COLOR_ID"],veh_clr_df["VEH_MAKE_ID"]).distinct()
            
            # Filter for top 25 states with highest number of offences
            top_states = veh_states_df.groupBy("DRVR_LIC_STATE_ID").count().sort("count",ascending=False).limit(25)
            top_states_list = top_states.select("DRVR_LIC_STATE_ID").rdd.flatMap(lambda x: x).collect()
            veh_states_df = veh_states_df.filter(veh_states_df["DRVR_LIC_STATE_ID"].isin(top_states_list))
            
            # Group the filtered DataFrame by VehicleMake and count the number of accidents
            vehicle_make_counts = veh_states_df.groupBy("VEH_MAKE_ID").count()
            
            # Sort the VehicleMake counts in descending order
            vehicle_make_counts = vehicle_make_counts.sort("count", ascending=False)
    
            # Select the top 5 Vehicle Makes
            top_vehicle_makes = vehicle_make_counts.select("VEH_MAKE_ID").rdd.flatMap(lambda x: x).take(5)
            
            # Print the top 5 Vehicle Makes
            #print("Top 5 Vehicle Makes: " + str(top_vehicle_makes))
            return(str(top_vehicle_makes))
        
        except Exception as e:
            print('Exception: ',e)
            
if __name__ == "__main__":
    try:
        #Creating Spark Sessions
        spark = SparkSession.builder.appName("CAR_CRASH_CASE_STUDY").getOrCreate()
        
        with open('car_crash_case_stud_config.yaml','r') as f:
            config_yml = yaml.load_all(f, Loader=yaml.FullLoader)
        
        df_dict={}
        
        for df in config_yml['case_study_config']:
            df_dict[df['file_name']]=df['path']
        
        
        #Reading DataSets
        charges_df  = spark.read.options(header='True', inferSchema='True').csv(df_dict['charges_df'])
        damages_df  = spark.read.options(header='True', inferSchema='True').csv(df_dict['damages_df'])
        endorse_df  = spark.read.options(header='True', inferSchema='True').csv(df_dict['endorse_df'])
        restrict_df = spark.read.options(header='True', inferSchema='True').csv(df_dict['restrict_df'])
        units_df    = spark.read.options(header='True', inferSchema='True').csv(df_dict['units_df'])
        primary_person_df = spark.read.options(header='True', inferSchema='True').csv(df_dict['primary_person_df'])
        
        #Data Cleaning - Dropping Duplicates
        charges_df  = charges_df.dropDuplicates()
        damages_df  = damages_df.dropDuplicates()
        endorse_df  = endorse_df.dropDuplicates()
        restrict_df = restrict_df.dropDuplicates()
        units_df    = units_df.dropDuplicates()
        primary_person_df = primary_person_df.dropDuplicates()
        
        CaseStudy = Case_Study()
        
        #Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?
        print("Number of crashes (accidents) in which number of persons killed are male : ",str(CaseStudy.Analysis_1(primary_person_df)))
        
        #Analysis 2: How many two wheelers are booked for crashes?
        print("Number of two wheelers booked for crashes : ",str(CaseStudy.Analysis_2(units_df)))
        
        #Analysis 3: Which state has highest number of accidents in which females are involved?
        print("State which has highest number of accidents in which females are involved : ",str(CaseStudy.Analysis_3(primary_person_df)))
        
        #Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        print("Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death : ",CaseStudy.Analysis_4(primary_person_df,units_df))
        
        #Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        print("For all the body styles involved in crashes, the top ethnic user group of each unique body style : ")
        CaseStudy.Analysis_5(primary_person_df,units_df).display()
        
        #Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        print("Among the crashed cars, the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash : ",CaseStudy.Analysis_6(primary_person_df,charges_df))
        
        #Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        print("Distinct Crash IDs count where No Damaged Property was observed and Damage Level is above 4 and car avails Insurance : ",CaseStudy.Analysis_7(units_df,damages_df))
        
        #Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
        print("Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences : ",CaseStudy.Analysis_8(endorse_df,charges_df,units_df,primary_person_df))
        

    except Exception as e:
        raise Exception(e)
        
############################################################## END OF FILE ##################################################################
    
    




