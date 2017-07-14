#rm(list=ls())

################
# Set up
################
# Install Packages - UNCOMMENT AND RUN THESE COMMANDS IF THE PACKAGES BELOW ARE NOT ALREADY INSTALLED

# install.packages("tidyr")
# install.packages("plyr")
# install.packages("dplyr")
# install.packages("treemap")

# Attach the packages to be used in the script
library(tidyr)
library(plyr)
library(dplyr)
library(treemap)

###################
# Step 1: Load Data
###################

# Set the working directory to the file location on your computer
# example: using Documents on the C drive
setwd("C:\\Users\\Public\\Documents\\")
# check the directory was set correctly
getwd()

# Import dataset
tas_categories <- read.csv("tas_categories.csv")
names(tas_categories)
table(tas_categories$treasury_account.reporting_agency_name)


# Subset Data To Include Only EPA Data'
table(tas_categories$treasury_account.reporting_agency_name=="Department of Transportation" & tas_categories$treasury_account.reporting_agency_id=="69")
dot_data <- tas_categories[which(tas_categories$treasury_account.reporting_agency_id=="69"),]

  # Drop unneeded dataset
  rm(tas_categories)

# Create Summary Tables of Spending by Object Class
gross_majorobjectclass <- data.frame(aggregate(dot_data$gross_outlay_amount_by_program_object_class_cpe, 
                                       by=list(dot_data$object_class.major_object_class_name), FUN=sum))

          # Create Treemap of EPA Spending by Major Object Class (Saved as PNG File)
          png(file="DOT_Spending_by_Object_Class.png")
          treemap(gross_majorobjectclass,
                  index = c("Group.1"),
                  vSize = "x",
                  vColor = "Group.1",
                  type = "index", 
                  palette = c("#0086c8", "#2869a4", "#143e64", "#2c2c2c", "#00caec", "#007faa"),
                  title="DOT Gross Spending by Major Object Class",
                  fontsize.title = 14)
          dev.off()

# Create Summary Table of DOT Spending by Secondary Object Class
  # Major Object Class: Other generates a blank Secondary Object Class, Replace Values
  dot_data$object_class.object_class_name <- as.character(dot_data$object_class.object_class_name)
          dot_data$object_class.object_class_name[which(dot_data$object_class.major_object_class_name=="Other" & 
                                                  dot_data$object_class.object_class_name=="")] <- "Other"  
dot_data$gross_outlay_absolute_val_cpe <- abs(dot_data$gross_outlay_amount_by_program_object_class_cpe)

gross_objectclass <- data.frame(aggregate(dot_data$gross_outlay_absolute_val_cpe, 
                                            by=list(dot_data$object_class.object_class_name), FUN=sum))


      # Create Treemap of DOT Spending by Secondary Object Class
      png(file="DOT_Spending_by_Secondary_Object_Class.png")  
      treemap(gross_objectclass, 
                index = c("Group.1"),
                vSize = "x", 
                vColor = "Group.1", 
                type="index", 
                palette = c("#0086c8", "#2869a4", "#143e64", "#00caec", "#007faa",
                            "#00b5db", "#aae1f4", "#e7f7f9", "#f4f4f4", "#2c2c2c",
                            "#414b57", "#6e747e", "#bcbec2", "#dedfe0",
                            "#024558", "#416878", "#6b8a97", "#b5cdd4", "#d8edf2"), 
                title = "DOT Spending by Secondary Object Class - Absolute Values", 
                fontsize.title = 14)
        dev.off()

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        