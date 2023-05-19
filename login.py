import yaml
import os
from sparkconnection import SparkConnection

#generate a function to store login credentials into variables from the yaml config file
def load_credentials():
    conf = yaml.safe_load(open('credentials.yml'))
    uid = conf['user']['uid']
    pwd = conf['user']['pwd']
    return uid, pwd

#function that generates a login menu that utilzes case-match statements for user input
def login_menu():
    uid, pwd = load_credentials()

    while True:
        print("---------------------Welcome to the Program------------------------")
        print("[1] Login with username/password")
        print("[2] Exit")
        print("-------------------------------------------------------------------")
        choice = input("Please enter your choice: ")

        match choice:
            case '1':
                login_success = login(uid, pwd)
                if login_success:
                    print("Login successful!")
                    main_menu()  # Call the menu function after successful login
                else:
                    print("Login failed. Please try again.")
            case '2':
                print("Thank you for using the program. Now Exiting...")
                break
            case _:
                print("Invalid choice. Please try again.")

#a login function that will check user input against login credentials with 3 opportunties to correctly enter
def login(uid, pwd):
    try:
        login = False
        count = 3

        while not login and count != 0:
            check_username = input("Enter your username: ")

            if check_username == uid:
                print("Username match!")
                check_password_attempts = 3

                while check_password_attempts != 0:
                    check_password = input("Enter your password: ")

                    if check_password == pwd:
                        print("You have entered the correct password!")
                        login = True
                        return True
                    else:
                        check_password_attempts -= 1
                        print("Incorrect password. Attempts remaining:", check_password_attempts)

                print("Exceeded maximum password attempts. Please try again.")
                count -= 1

            else:
                print("Username does not match!")
                count -= 1

    except Exception as e:
        print("User login error!")
        raise e

    return False

#Main menu is called after successful login
def main_menu():
    while True:
        print("\n---------------------Main Menu-----------------------------")
        print("[1] Run AdventureWorks2019 database conversion")
        print("[2] Run the Spark program")
        print("[3] Exit")
        print("-------------------------------------------------------------")
        choice = input("Please enter your choice: ")

        match choice:
            case '1':
                # Execute the dbconnector.py script
                os.system("python dbconnector.py")
            case '2':
                sc = SparkConnection()
                sc.spark_menu()
            case '3':
                print("Exiting...")
                break
            case _:
                print("Invalid choice. Please try again.")