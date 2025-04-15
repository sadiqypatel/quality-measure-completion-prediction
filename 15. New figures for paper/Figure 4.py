# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import numpy as np
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from matplotlib.patches import Patch
import matplotlib.pyplot as plt
import io
from PIL import Image
import matplotlib.pyplot as plt

# COMMAND ----------

import pandas as pd

# Create the DataFrame based on the data provided
data = {
    'Measure': ['Poverty rate', 'APRN/PA supply', 'Mental health facilities', 
                'High school graduation rate', 'Air quality', 'SSI recipient', 
                'Substance use facilities', 'Urgent care clinics', 'Population density', 
                'TANF recipient', 'SSDI recipient', 'English proficiency', 'Household income'],
    'Prenatal': [2.5, 6.2, 10.8, 12.5, 10.9, 11.0, 10.3, 9.8, 10.4, 10.0, 11.0, 11.1, 12.5],
    'LBP': [6.8, 12.2, 22.8, 32.7, 38.2, 38.7, 41.3, 42.6, 43.6, 44.2, 44.6, 44.6, 44.6],
    'PCR': [3.0, 3.5, 17.5, 22.6, 25.0, 27.8, 30.4, 30.4, 31.3, 31.3, 31.5, 31.5, 31.5],
    'PBH': [11.3, 13.5, 16.7, 17.8, 19.2, 19.2, 17.6, 20.5, 20.6, 20.6, 21.2, 21.2, 21.2],
    'SPC adherence': [8.1, 10.1, 13.3, 17.3, 19.5, 20.0, 22.3, 23.4, 24.1, 24.3, 24.5, 24.5, 24.5],
    'SPD adherence': [5.6, 11.8, 16.1, 19.3, 18.1, 18.7, 18.9, 21.0, 21.7, 21.8, 22.6, 22.6, 22.6]
}

df = pd.DataFrame(data)

# Display the created DataFrame
print(df)

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors as mcolors

# Columns to plot
groups = ['LBP', 'Prenatal', 'PBH', 'SPC adherence', 'SPD adherence', 'PCR']
measures = df['Measure'].tolist()

# Prepare data for plotting by calculating differences for stacking
stacked_data = np.zeros((len(measures), len(groups)))
for i, group in enumerate(groups):
    stacked_data[:, i] = df[group].diff().fillna(df[group])  # Calculate differences for stacking

# Assign colors based on groups
red_measures = ['SSI recipient', 'SSDI recipient', 'English proficiency', 'Household income', 'TANF recipient']
blue_measures = ['Poverty rate', 'Air quality', 'Population density', 'High school graduation rate']
green_measures = ['APRN/PA supply', 'Mental health facilities', 'Substance use facilities', 'Urgent care clinics']

# Define gradients for colors
red_colors = plt.cm.Reds(np.linspace(0.5, 0.9, len(red_measures)))
blue_colors = plt.cm.Blues(np.linspace(0.5, 0.9, len(blue_measures)))
green_colors = plt.cm.Greens(np.linspace(0.5, 0.9, len(green_measures)))

# Combine gradients into a color mapping
color_mapping = {}
for measure, color in zip(red_measures, red_colors):
    color_mapping[measure] = color
for measure, color in zip(blue_measures, blue_colors):
    color_mapping[measure] = color
for measure, color in zip(green_measures, green_colors):
    color_mapping[measure] = color

# Create the stacked bar chart
fig, ax = plt.subplots(figsize=(13, 9))  # Control figure size here
for i, measure in enumerate(measures):
    color = color_mapping.get(measure, 'gray')  # Default to gray if not mapped
    if i == 0:
        ax.barh(groups, stacked_data[i], label=measure, color=color)
    else:
        ax.barh(groups, stacked_data[i], left=np.sum(stacked_data[:i], axis=0), label=measure, color=color)

# Add vertical group labels with 2-line formatting
group_labels = [
    'Chronic disease management',
    'Care\ncoordination',
    'Unnecessary\ncare',
    'Maternal and\nchild health'
]
group_label_positions = [3, 5, 0, 1]  # Adjust positions based on group spacing
for pos, label in zip(group_label_positions, group_labels):
    ax.text(-8, pos, label, va='center', ha='center', rotation=90, fontsize=11)  # Customize font size

# Make layout adjustments but keep graph area intact
#plt.subplots_adjust(left=0.15, right=0.85, top=0.11, bottom=0.1)

# Customize font sizes
title_font = {'fontsize': 12}  # Title font size
axis_label_font = {'fontsize': 11}  # Axis label font size
tick_label_font = {'fontsize': 11}  # Tick label font size
legend_font_size = 11  # Legend font size

# Set x-axis limits and labels
ax.set_xlim(0, 50)  # Stretch x-axis to 0-60
ax.set_xlabel('Cumulative percentage improvement', fontdict=axis_label_font)

# Set title
ax.set_title('Figure 4(b): Stacked bar chart of preventive care gap with ≥10% improvement', fontdict=title_font)

# Keep original y-axis labels
ax.set_yticks(range(len(groups)))
ax.set_yticklabels(groups, fontdict=tick_label_font)

# # Add legend
# measure_legend =ax.legend(title='SDoH factors', bbox_to_anchor=(0.74, 1), loc='upper left', prop={'size': legend_font_size})
# ax.add_artist(measure_legend)  # Keep the original legend on the plot


# Add the custom SDoH legend
import matplotlib.patches as mpatches

# Create legend patches
area_sdoh_patch = mpatches.Patch(color=blue_colors[-1], label='Area SDoH')  # Use the last blue gradient
healthcare_access_patch = mpatches.Patch(color=green_colors[-1], label='Healthcare Access')  # Use the last green gradient
individual_sdoh_patch = mpatches.Patch(color=red_colors[-1], label='Individual SDoH')  # Use the last red gradient

# # Add the custom legend below the main legend
# plt.legend(handles=[area_sdoh_patch, healthcare_access_patch, individual_sdoh_patch], 
#            title="SDoH categories", bbox_to_anchor=(0.74, 0.585), loc='upper left', prop={'size': legend_font_size})


# Save to in-memory buffer as PNG with high DPI
buf = io.BytesIO()
plt.savefig(buf, format='png', dpi=1000, bbox_inches='tight')
buf.seek(0)

# Display the image inline
img = Image.open(buf)
display(img)


# COMMAND ----------

import pandas as pd

# Create the DataFrame with renamed columns
data = {
    "Measure": [
        "Poverty rate", "APRN/PA supply", "Mental health facilities",
        "High school graduation rate", "Air quality", "SSI recipient",
        "Substance use facilities", "Urgent care clinics", "Population density",
        "TANF recipient", "SSDI recipient", "English speaker", "Household income"
    ],
    "Maternal and child health": [1.8, 3.5, 4.9, 5.3, 5.2, 5.0, 5.1, 5.4, 5.6, 5.5, 5.8, 5.8, 5.9],
    "Chronic disease management": [5.2, 7.3, 9.6, 11.7, 12.3, 12.5, 12.6, 13.8, 14.1, 14.1, 14.4, 14.5, 14.5],
    "Unnecessary care": [6.8, 12.2, 22.8, 32.7, 38.2, 38.7, 41.3, 42.6, 43.6, 44.2, 44.6, 44.6, 44.6],
    "Care coordination": [3.0, 3.5, 17.5, 22.6, 25.0, 27.8, 30.3, 30.4, 31.3, 31.3, 31.5, 31.5, 31.5],
    "Behavioral health": [1.1, 0.8, 0.7, 1.7, 2.1, 2.0, 1.8, 2.2, 2.1, 2.1, 2.1, 2.1, 2.1]
}

# Create the pandas DataFrame
df = pd.DataFrame(data)

# Display the created DataFrame
print(df)

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors as mcolors

# Columns to plot
#groups = ['Care coordination', 'Chronic disease management', 'Unnecessary care', 'Preventive care', 'Behavioral health']
groups = ['Behavioral health', 'Unnecessary care', 'Maternal and child health', 'Chronic disease management', 'Care coordination']


measures = df['Measure'].tolist()

# Prepare data for plotting by calculating differences for stacking
stacked_data = np.zeros((len(measures), len(groups)))
for i, group in enumerate(groups):
    stacked_data[:, i] = df[group].diff().fillna(df[group])  # Calculate differences for stacking

# Assign colors based on groups
red_measures = ['SSI recipient', 'SSDI recipient', 'English proficiency', 'Household income', 'TANF recipient']
blue_measures = ['Poverty rate', 'Air quality', 'Population density', 'High school graduation rate']
green_measures = ['APRN/PA supply', 'Mental health facilities', 'Substance use facilities', 'Urgent care clinics']

# Define gradients for colors
red_colors = plt.cm.Reds(np.linspace(0.5, 0.9, len(red_measures)))
blue_colors = plt.cm.Blues(np.linspace(0.5, 0.9, len(blue_measures)))
green_colors = plt.cm.Greens(np.linspace(0.5, 0.9, len(green_measures)))

# Combine gradients into a color mapping
color_mapping = {}
for measure, color in zip(red_measures, red_colors):
    color_mapping[measure] = color
for measure, color in zip(blue_measures, blue_colors):
    color_mapping[measure] = color
for measure, color in zip(green_measures, green_colors):
    color_mapping[measure] = color

# Create the stacked bar chart
fig, ax = plt.subplots(figsize=(13, 9))  # Control figure size here
for i, measure in enumerate(measures):
    color = color_mapping.get(measure, 'gray')  # Default to gray if not mapped
    if i == 0:
        ax.barh(groups, stacked_data[i], label=measure, color=color)
    else:
        ax.barh(groups, stacked_data[i], left=np.sum(stacked_data[:i], axis=0), label=measure, color=color)

# Customize font sizes
title_font = {'fontsize': 12}  # Title font size
axis_label_font = {'fontsize': 11}  # Axis label font size
tick_label_font = {'fontsize': 11}  # Tick label font size
legend_font_size = 9  # Legend font size

# Set x-axis limits and labels
ax.set_xlim(0, 50)  # Stretch x-axis to 0-60
ax.set_xlabel('Cumulative percentage improvement', fontdict=axis_label_font)

# Set title
ax.set_title('Figure 4(a): Stacked bar chart of percentage improvement by type of preventive care gap', fontdict=title_font)

# Keep original y-axis labels
ax.set_yticks(range(len(groups)))
ax.set_yticklabels(groups, fontdict=tick_label_font)

# Add legend
measure_legend =ax.legend(title='SDoH factors', bbox_to_anchor=(0.72, 1), loc='upper left', prop={'size': legend_font_size})
ax.add_artist(measure_legend)  # Keep the original legend on the plot


# Add the custom SDoH legend
import matplotlib.patches as mpatches

# Create legend patches
area_sdoh_patch = mpatches.Patch(color=blue_colors[-1], label='Area SDoH')  # Use the last blue gradient
healthcare_access_patch = mpatches.Patch(color=green_colors[-1], label='Healthcare Access')  # Use the last green gradient
individual_sdoh_patch = mpatches.Patch(color=red_colors[-1], label='Individual SDoH')  # Use the last red gradient

# Add the custom legend below the main legend
plt.legend(handles=[area_sdoh_patch, healthcare_access_patch, individual_sdoh_patch], 
           title="SDoH categories", bbox_to_anchor=(0.72, 0.585), loc='upper left', prop={'size': legend_font_size})


# Save to in-memory buffer as PNG with high DPI
buf = io.BytesIO()
plt.savefig(buf, format='png', dpi=1000, bbox_inches='tight')
buf.seek(0)

# Display the image inline
img = Image.open(buf)
display(img)


# COMMAND ----------

