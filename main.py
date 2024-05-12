import os, shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, upper, expr, trim, when

output_directory = "./outputs"

#Καθώς η συνάρτηση dataframe.write.csv δημιουργεί έναν φάκελο με ονομασίες αρχείων που δεν είναι κατανοητές για τον τελικό χρήστη δημιουργήσαμε 
#μια συνάρτηση που παίρνει τα αρχεία αυτής της συνάρτησης τα μετονομάζει και τα μετακινεί στο φάκελο outputs

def write_to_csv(df, output_csv_path, new_file_name):
    #Χρησιμοποιούμε αυτή τη συνάρτηση για να γράψουμε το dataframe σε αρχείο csv
    #To αρχείο csv γράφεται στο output_csv_path το οποίο δηλώνεται από εμάς στην είσοδο της συνάρτησης 
    df.write.csv(output_csv_path, header=True, mode="overwrite")
    
    #Δηλώνουμε πού βρίσκεται το αρχείο εξόδου
    files = os.listdir(output_csv_path)

    # Find the CSV file (it may have a different name prefix, so use a pattern)
    #Το αρχείο csv έχει συγκεκριμένη ονομασία.
    #ΞΕκινάει με part- και τελειώνει σε .csv οπότε βρίσκουμε το συγκεκριμένο αρχείο μέσα στον φάκελο (είναι μοναδικό)
    csv_file = [file for file in files if file.endswith(".csv") and file.startswith("part-")][0]
    
    #μετονομάζουμε το αρχείο
    # Rename the CSV file to your desired output file name
    new_csv_file = os.path.join(output_csv_path, "output.csv")
    os.rename(os.path.join(output_csv_path, csv_file), new_csv_file)

    print(f"Output CSV file renamed to {new_csv_file}")

    # Move the CSV file to the 'outputs' directory
    #Μεταφέρουμε το csv στον φάκελο  'outputs'
    shutil.move(new_csv_file, os.path.join(output_directory, new_file_name + ".csv"))

    # Remove the original directory
    #Σβήνουμε τον αρχικό φάκελο
    shutil.rmtree(output_csv_path)

    print("File moved to 'outputs' directory and original directory removed.")


def remove_percentage_sign(df):
    #δήλωση στηλών στις οποίες πρέπει να αφαιρεθεί το %
    columns_to_remove_percent_sign = [

        "T1_Att_Kill_Perc",
        "T1_Att_Eff",
        "T1_Att_Sum",
        "T2_Srv_Eff",
        "T2_Rec_Pos",
        "T2_Rec_Perf",
        "T2_Att_Kill_Perc",
        "T2_Att_Eff",
        "T2_Att_Sum"
    ]
    #για κάθε στήλη που έχει όνομα που βρίσκεται στον πίνακα κάνουμε replace to % με κενό string
    for column_name in columns_to_remove_percent_sign:
        df = df.withColumn(
            column_name,
            regexp_replace(
                col(column_name),
                "%",
                ""
            ).cast("double")
        )

    print("Removed percentage sign from dataframe.")
    #εμφανίζω το αποτέλεσμα στην οθόνη χωρίς περικοπή
    df.show(truncate=False)


def team_names_to_upper(df):
    name_columns = [
        "Team_1",
        "Team_2",
    ]
    #για κάθε στήλη που έχει όνομα που βρίσκεται στον πίνακα μετατρέπω τους χαρακτήρες σε κεφαλαία
    for column_name in name_columns:
        df = df.withColumn(column_name, upper(col(column_name)))

    print("Turned team names to uppercase.")
    df.show(truncate=False)

def games_per_team(df):
    #Κάνουμε select όλες τις ομάδες από Team_1 και Team_2 και τις κάνουμε union
    teams_union = df.select(col("Team_1").alias("Team")).union(
        df.select(col("Team_2").alias("Team")))
    # Count the number of games played by each team (both as home and away)
    #Κάνουμε groupBy στη στήλη Team και count στο αποτέλεσμα για να βρούμε τον αριθμό των παιχνιδιών που έχει παίξει κάθε ομάδα
    team_games_count = teams_union.groupBy("Team").count().withColumnRenamed("count", "Number_Of_Games")
    team_games_count.show(truncate=False)
    #Γράφουμε το αποτέλεσμα σε ένα αρχείο csv
    write_to_csv(team_games_count, "./erotima_6", "erotima_6")


# Press the green button in the gutter to run the script.
def main():
    #Αρχικοποίηση Spark Session
    spark = SparkSession.builder.appName("Project").getOrCreate()
    #Δημιουργία φακέλου για τα output csv
    os.makedirs(output_directory, exist_ok=True)
    input_csv_path = "./Mens-Volleyball-PlusLiga-2008-2023.csv"
    #Ανάγνωση input csv και δημιουργία dataframe
    base_data_frame = spark.read.csv(input_csv_path, header=True, inferSchema=True)

    # Zitoumeno 1 - erotima 1
    remove_percentage_sign(base_data_frame)

    # Zitoumeno 1 - erotima 2
    team_names_to_upper(base_data_frame)

    # Zitoumeno 1 - erotima 3
    #αποθηκεύω το count σε μια μεταβλητή
    #ο αριθμός των παιχνιδιών είναι όλες οι γραμμές του dataframe (μια γραμμή ανά παιχνίδι)
    number_of_games = base_data_frame.count()
    print(number_of_games)

    # Zitoumeno 1 - erotima 4
    #Προσθέτω στο dataframe μια στήλη που περιέχει το άθροισμα των T1_Score και T2_Score
    #withColumn documentation: Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
    #The column expression must be an expression over this DataFrame; attempting to add a column from some other DataFrame will raise an error.
    df_with_sum_of_sets = base_data_frame.withColumn("Sum_Of_Sets", col("T1_Score") + col("T2_Score"))
    df_with_sum_of_sets.show(truncate=False)

    
    # Zitoumeno 1 - erotima 5
    #Χρησιμοποιώντας το dataframe του ερωτήματος 4
    #Καθώς θέλουμε να πάρουμε τον αριθμό των παιχνιδιών ανάλογα με τον αριθμό των sets κάνουμε groupBy το dataframe με τη στήλη Sum_Of_Sets και το count βγάζει 
    #τον αριθμό των records που πήραμε από το groupBy  και μετονομάζουμε τη στήλη count με Number_Of_Games
    games_per_sum_of_sets = df_with_sum_of_sets.groupBy("Sum_Of_Sets").count().withColumnRenamed("count",
                                                                                                 "Number_Of_Games")
    games_per_sum_of_sets.show(truncate=False)

    write_to_csv(games_per_sum_of_sets, "./erotima_5", "erotima_5")

    # Zitoumeno 1 - erotima 6
    games_per_team(base_data_frame)

    # Zitoumeno 2 - part1
    #Κάνουμε groupBy στις στήλες Team_1 και Team_1 και count στο αποτέλεσμα για να βρούμε τον αριθμό των παιχνιδιών που έχει παίξει κάθε ομάδα ως γηπεδούχος και ως φιλοξενούμενη
    #(εντός και εκτός έδρας)
    team_1_counts = base_data_frame.groupBy("Team_1").count().withColumnRenamed("count", "Home_Count")
    team_2_counts = base_data_frame.groupBy("Team_2").count().withColumnRenamed("count", "Away_Count")

    #θα διατηρηθούν όλες οι εγγραφές και από τα δύο DataFrames, και οι εγγραφές που δεν έχουν αντιστοίχιση θα έχουν τιμές NULL
    total_counts = team_1_counts.join(team_2_counts, team_1_counts.Team_1 == team_2_counts.Team_2, "outer") \
        .select( #επιλέγει τα πεδία που θα συμπεριληφθούν στο τελικό DataFrame
        #επιλέγει την τιμή από τη στήλη Team_1, αλλά αν είναι NULL, τότε επιλέγει την τιμή από τη στήλη Team_2. Το αποτέλεσμα ονομάζεται "Team". Ομοίως και στα υπόλοιπα
        expr("IFNULL(Team_1, Team_2)").alias("Team"), 
        expr("IFNULL(Home_Count, 0)").alias("Home"),
        expr("IFNULL(Away_Count, 0)").alias("Away"),
        #Δημιουργεί ένα νέο πεδίο "Total", το οποίο είναι η συνολική ποσότητα ως άθροισμα των ποσοτήτων στις στήλες Home_Count και Away_Count
        expr("IFNULL(Home_Count, 0) + IFNULL(Away_Count, 0)").alias("Total")
    )
    #ταξινόμηση του dataframe ως προς το Total με φθίνουσα σειρά
    total_counts = total_counts.orderBy(col("Total").desc())

    total_counts.show(truncate=False)

    # Zitoumeno 2 - part 2
    #Φτιάχνουμε ένα dataframe με όλες τις ομάδες, τα sets που κέρδισαν, τα sets που έχασαν
    sets_info = base_data_frame.select(
        col("Team_1").alias("Team"),
        col("T1_Score").cast("int").alias("Sets_Won"),
        col("T2_Score").cast("int").alias("Sets_Lost")
    ).union(
        base_data_frame.select(
            col("Team_2").alias("Team"),
            col("T2_Score").cast("int").alias("Sets_Won"),
            col("T1_Score").cast("int").alias("Sets_Lost")
        )
    )
    #Κάνουμε groupBy ανάλογα με την ομάδα με τη βοήθεια της συνάρτησης agg που συναθροίζει τα ομαδοποιημένα δεδομένα
    team_sets_info = sets_info.groupBy("Team").agg(
        expr("sum(Sets_Won)").alias("Total_Sets_Won"),
        expr("sum(Sets_Lost)").alias("Total_Sets_Lost")
    )

    team_sets_info.show(truncate=False)


    # Zitoumeno 2 - part 3
    #Ομοίως με το προηγούμενο
    score_info = base_data_frame.select(
        col("Team_1").alias("Team"),
        col("T1_Sum").cast("int").alias("Points_Scored"),
        col("T2_Sum").cast("int").alias("Points_Conceded")
    ).union(
        base_data_frame.select(
            col("Team_2").alias("Team"),
            col("T2_Sum").cast("int").alias("Points_Scored"),
            col("T1_Sum").cast("int").alias("Points_Conceded")
        )
    )
    
    team_points_info = score_info.groupBy("Team").agg(
        expr("sum(Points_Scored)").alias("Total_Points_Scored"),
        expr("sum(Points_Conceded)").alias("Total_Points_Conceded")
    )

    team_points_info.show(truncate=False)

    #Υπολογισμών συνολικά κερδισμένων αγώνων
    df_with_winner_name = base_data_frame.withColumn(
        "Winning_Team",
        when(col("Winner") == 0, col("Team_1")).otherwise(col("Team_2"))
    )
        
    # Count the number of wins for each team
    team_wins_count = df_with_winner_name.groupBy("Winning_Team").count().withColumnRenamed("count", "Wins")

    # Sort the result by the number of wins in descending order
    team_wins_count = team_wins_count.orderBy(col("Wins").desc())

    team_wins_count.show(truncate=False)
    
    # Combine all 3 in on data frame
    #Κάνουμε ένα join των τριών τελευταίων dataframes
    total_counts = (total_counts.join(
        team_sets_info,
        trim(total_counts.Team) == trim(team_sets_info.Team),
        "outer"
    ).join(
        team_points_info,
        trim(total_counts.Team) == trim(team_points_info.Team),
        "outer"
    ).join(
        team_wins_count,
        trim(total_counts.Team) == trim(team_wins_count.Winning_Team),
        "outer"
    ).select(
        total_counts.Team,
        total_counts.Home,
        total_counts.Away,
        (total_counts.Home + total_counts.Away).alias("Total"),
        team_sets_info.Total_Sets_Won,
        team_sets_info.Total_Sets_Lost,
        team_points_info.Total_Points_Scored,
        team_points_info.Total_Points_Conceded,
        team_wins_count.Wins
    ).orderBy(col("Wins").desc()))
    

    total_counts.show(truncate=False)
    #Γράφουμε το τελικό dataframe σε csv
    write_to_csv(total_counts, "./part2", "part2")
    
main()