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

# Data for the table
data = {
    "AMM acute": [0.000, 0.178, 0.486, 0.213, 0.000, 0.131, 0.818, 0.238, 1.091, 1.654, 0.302, 0.937, 0.889],
    "AMM continuation": [0.003, 0.003, 0.102, 0.076, 0.000, 0.053, 0.591, 0.069, 1.678, 0.439, 0.026, 0.608, 1.304],
    "FUM30": [0.000, 0.297, 0.815, 0.070, 0.005, 0.087, 0.088, 0.374, 0.589, 1.306, 0.199, 1.133, 0.420],
    "Postpartum": [0.034, 0.086, 0.519, 2.445, 0.073, 0.745, 0.568, 0.297, 0.282, 2.733, 0.297, 0.161, 1.070],
    "Prenatal": [0.054, 0.059, 3.210, 0.693, 0.069, 0.540, 4.957, 0.511, 3.587, 2.454, 0.703, 2.586, 2.794],
    "WCV": [0.001, 0.006, 1.077, 0.431, 0.069, 0.257, 0.974, 0.334, 0.034, 0.054, 0.155, 0.083, 0.158],
    "LBP": [0.009, 0.493, 9.977, 0.764, 0.037, 4.098, 12.458, 3.668, 5.863, 6.833, 1.710, 13.221, 6.668],
    "PBH": [0.000, 0.539, 1.077, 0.898, 0.000, 0.180, 1.975, 2.873, 3.232, 11.311, 0.898, 3.770, 1.257],
    "SPC utilization": [0.002, 0.051, 0.030, 0.035, 0.000, 0.132, 0.328, 0.105, 0.164, 0.242, 0.040, 0.253, 0.113],
    "SPD utilization": [0.000, 0.099, 0.050, 0.053, 0.011, 0.520, 1.218, 0.261, 0.810, 0.480, 0.079, 1.210, 1.059],
    "SPD adherence": [0.000, 0.885, 0.761, 0.164, 0.050, 2.013, 4.170, 2.371, 6.113, 5.635, 0.516, 2.321, 2.554],
    "SPC adherence": [0.000, 0.440, 0.512, 0.429, 0.024, 2.690, 4.178, 1.143, 2.071, 8.094, 0.464, 3.654, 1.667],
    "PCR": [0.000, 0.977, 4.883, 0.000, 0.000, 1.682, 5.782, 0.334, 6.488, 3.039, 1.736, 2.116, 2.930],
}

# SDoH factors as row labels
rows = [
    "Household income", 
    "SSDI recipient", 
    "SSI recipient", 
    "TANF recipient", 
    "English proficiency", 
    "Substance use facilities", 
    "Mental health facilities", 
    "Urgent care clinics", 
    "APRN/NP supply", 
    "Poverty rate", 
    "Population density", 
    "High school graduation rate", 
    "Air quality"
]

# Create the DataFrame
sdoh_hedis_df = pd.DataFrame(data, index=rows)

# Display the DataFrame
print(sdoh_hedis_df)

# COMMAND ----------

import pandas as pd
import numpy as np

# Assuming sdoh_hedis_df is your DataFrame
# Replace this with your actual DataFrame creation or loading code
# sdoh_hedis_df = pd.read_csv('your_file.csv')

# Flatten all numeric values into a single array
numeric_values = sdoh_hedis_df.select_dtypes(include=[np.number]).values.flatten()

# Sorting the numeric values for proper quantile splitting
sorted_values = np.sort(numeric_values)

# Define the ranges based on the criteria
ranges = {
    "0-0.25": (0, 0.25),
    "0.25-0.5": (0.25, 0.5),
}

# Handle values greater than or equal to 0.5 by splitting into 3 equal-sized groups
values_above_0_5 = sorted_values[sorted_values >= 0.5]
num_splits = 3
split_points = np.linspace(0.5, values_above_0_5.max(), num_splits + 1)

# Add these groups to the ranges dictionary
for i in range(num_splits):
    lower = split_points[i]
    upper = split_points[i + 1]
    ranges[f"{lower:.2f}-{upper:.2f}"] = (lower, upper)

# Count the number of observations in each group
group_counts = {
    key: ((sorted_values >= low) & (sorted_values < high)).sum()
    if i < len(ranges) - 1 else ((sorted_values >= low) & (sorted_values <= high)).sum()
    for i, (key, (low, high)) in enumerate(ranges.items())
}

# Print ranges and their corresponding counts
for group, count in group_counts.items():
    print(f"Range {group}: {count} observations")


# COMMAND ----------

import pandas as pd
import numpy as np

# Assuming sdoh_hedis_df is your DataFrame
# Replace this with your actual DataFrame creation or loading code
# sdoh_hedis_df = pd.read_csv('your_file.csv')

# Flatten all numeric values into a single array
numeric_values = sdoh_hedis_df.select_dtypes(include=[np.number]).values.flatten()

# Sorting the numeric values for proper quantile splitting
sorted_values = np.sort(numeric_values)

# Manually define the custom split points
custom_split_points = [0.0, 0.25, 0.75, 2.5, 13.22]  # Adjusted ranges as per the request

# Define ranges based on custom split points
ranges = {
    f"{custom_split_points[i]:.2f}-{custom_split_points[i+1]:.2f}": (custom_split_points[i], custom_split_points[i+1])
    for i in range(len(custom_split_points) - 1)
}

# Count the number of observations in each group
group_counts = {
    key: ((sorted_values >= low) & (sorted_values < high)).sum()
    if i < len(ranges) - 1 else ((sorted_values >= low) & (sorted_values <= high)).sum()
    for i, (key, (low, high)) in enumerate(ranges.items())
}

# Print ranges and their corresponding counts
for group, count in group_counts.items():
    print(f"Range {group}: {count} observations")


# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm


# Define the cutoffs and colors for the heatmap
cutoffs = [0.00, 0.25, 0.75, 2.5, 13.22]
# Updated colors with better contrast
colors = ['#d6eaf8', '#aed6f1', '#5dade2', '#2e86c1', '#154360']  # Light to dark appealing blue shades
cmap = ListedColormap(colors)
norm = BoundaryNorm(cutoffs, len(colors))

# Transpose the DataFrame to map SDoH factors to the x-axis and HEDIS to the y-axis
transposed_df = sdoh_hedis_df.T  # Transpose: SDoH becomes columns (x-axis), HEDIS becomes rows (y-axis)

# Create the heatmap
plt.figure(figsize=(16, 11))  # Wide enough for SDoH factors on x-axis
# Create the heatmap
heatmap = sns.heatmap(
    data=transposed_df,  # Transposed DataFrame (HEDIS on y-axis, SDoH on x-axis)
    annot=True,          # Annotate cells with their values
    fmt=".3f",           # Format values as floats with 3 decimal places
    cmap=cmap,           # Apply the custom colormap
    norm=norm,           # Normalize values based on the cutoffs
    cbar=False,          # Disable default colorbar
    linewidths=0.5,      # Add spacing between boxes
)

# Adjust the x-axis labels (SDoH factors)
x_labels = transposed_df.columns  # Get SDoH factor names
heatmap.set_xticks([i + 0.5 for i in range(len(x_labels))])  # Center tick positions
heatmap.set_xticklabels(
    x_labels, fontsize=11, rotation=45, ha='right'  # Adjust x-axis label font size and rotation
)

# Adjust the y-axis labels (HEDIS measures)
y_labels = transposed_df.index  # Get HEDIS measure names
heatmap.set_yticks([i + 0.5 for i in range(len(y_labels))])  # Center tick positions
heatmap.set_yticklabels(
    y_labels, fontsize=11, rotation=0, va='center'  # Adjust y-axis label font size
)

legend_colors = [colors[i] for i in range(len(colors))]
legend_labels = [  # Ensure the length matches the number of colors
    "0.00-0.24% (n=55)", 
    "0.25-0.49% (n=46)", 
    "0.50-0.74% (n=38)", 
    "0.75-0.99% (n=30)"
]

for i in range(len(legend_labels)):  # Use len(legend_labels) to avoid index issues
    plt.scatter([], [], color=legend_colors[i], label=legend_labels[i])

# Adjust legend location and font size
legend = plt.legend(
    title='Percentage improvement',  # Title for legend
    bbox_to_anchor=(1.26, 1),  # Control horizontal and vertical position of the legend
    loc='upper right',  # Keep it in the top-right corner
    fontsize=8.5,        # Font size for legend labels
    title_fontsize=8.5,  # Font size for legend title
    frameon=False       # Remove box around legend
)

# Add a title and axis labels with customizable font sizes
plt.title("Figure 3(a): Heatmap of percentage improvement in preventive care gap completion", fontsize=12, pad=20)  # Title font size
# plt.xlabel('SDoH Factors', fontsize=11)  # Label for SDoH Factors (x-axis font size)
# plt.ylabel('HEDIS Measures', fontsize=11)  # Label for HEDIS Measures (y-axis font size)

# Stretch boxes by adjusting the aspect ratio
plt.gca().set_aspect(1.0, adjustable='box')  # Adjust aspect ratio for equal box sizes

# Define group categories and positions for the grouped labels
group_categories = ["Individual SDoH", "Healthcare Access", "Area SDoH"]
group_positions = [
    (0, 2.5),  # Span for "Individual SDoH" (first five columns)
    (3, 8),  # Span for "Healthcare Access" (next four columns)
    (8, 13)  # Span for "Area SDoH" (final four columns)
]

# Add grouped labels just below the x-axis labels
for idx, (start, end) in enumerate(group_positions):
    plt.text(
        x=(start + end) / 2,  # Position in the center of the group
        y=16,               # Adjust this value to place the labels lower
        s=group_categories[idx],  # Group label text
        ha='center', fontsize=11, weight='regular'
    )

plt.xlabel("", fontsize=12)  # Replace "SDoH Factor" with your desired label text

# Add more space below the x-axis to accommodate the group labels
plt.subplots_adjust(bottom=0.2)  # Increase bottom margin to ensure everything fits

# Save to in-memory buffer as PNG with high DPI
buf = io.BytesIO()
plt.savefig(buf, format='png', dpi=1000, bbox_inches='tight')
buf.seek(0)

# Display the image inline
img = Image.open(buf)
display(img)


# COMMAND ----------

# Create the boxplot with a stripplot overlay
plt.figure(figsize=(16, 11))  # Control the figure size here: (width, height)

custom_palette = ['#ccffcc', '#99ff99', '#66cc66', '#339933']


# Flatten the DataFrame for plotting
data = sdoh_hedis_df.stack().reset_index()
data.columns = ['SDoH Factor', 'HEDIS Measure', 'Percentage Improvement']

# Customize labels and ticks
plt.xticks(rotation=45, ha="right", fontsize=12)  # Rotate x-axis labels for readability
plt.xlabel("", fontsize=12)  # Customize x-axis label font size
plt.ylabel("Percentage improvement", fontsize=12)  # Customize y-axis label font size

# Adjust the y-axis range (optional)
plt.ylim(0, 12)  # Adjust this as needed to control the visible range of the plot

# Define group categories and positions for the grouped labels
group_categories = ["Individual SDoH", "Healthcare access", "Area SDoH"]
group_positions = [
    (0, 2.5),  # Span for "Individual SDoH" (first five columns)
    (4, 8),  # Span for "Healthcare Access" (next four columns)
    (8, 13)  # Span for "Area SDoH" (final four columns)
]

# Add grouped labels just below the x-axis labels
for idx, (start, end) in enumerate(group_positions):
    plt.text(
        x=(start + end) / 2,  # Position in the center of the group
        y=-3,               # Adjust this value to place the labels lower
        s=group_categories[idx],  # Group label text
        ha='center', fontsize=12, weight='regular'
    )

# Apply custom palette to the boxplot
sns.boxplot(
    data=data,
    x='SDoH Factor',
    y='Percentage Improvement',
    palette=custom_palette,  # Use the custom blue palette
    showfliers=False
)

# Overlay stripplot with a slight contrast
sns.stripplot(
    data=data,
    x='SDoH Factor',
    y='Percentage Improvement',
    color='black',  # Keep black dots for individual data points
    alpha=0.6,
    jitter=True
)

# Add more space below the x-axis to accommodate the group labels
plt.subplots_adjust(bottom=0.2)  # Increase bottom margin to ensure everything fits

# Add a title
plt.title("Figure 3(b): Boxplot of percentage improvement in preventive care gap completion by SDoH factor", fontsize=12, pad=20)

plt.xlabel("", fontsize=12)  # Replace "SDoH Factor" with your desired label text

# Save to in-memory buffer as PNG with high DPI
buf = io.BytesIO()
plt.savefig(buf, format='png', dpi=1000, bbox_inches='tight')
buf.seek(0)

# Display the image inline
img = Image.open(buf)
display(img)


# COMMAND ----------

