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
import io
from PIL import Image
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC sensitivity

# COMMAND ----------

# Define group colors
group_colors = {
    "Care\ncoordination": "purple",
    "Chronic disease\nmanagement": "green",
    "Unnecessary care": "brown",
    "Maternal and\nchild health": "cyan",  # Split into two lines
    "Behavioral\nhealth": "pink"
}

# Define point colors for SDoH, non-SDoH, and Random Selection
point_colors = {"SDoH": "blue", "non-SDoH": "orange", "Random Selection": "red"}

# Categories and measures (corrected PBH group)
categories = {
    "Care\ncoordination": ["PCR"],
    "Chronic disease\nmanagement": ["SPC adherence", "SPD adherence", "SPC utilization", "SPD utilization", "PBH"],
    "Unnecessary care": ["LBP"],
    "Maternal and\nchild health": ["WCV", "Prenatal", "Postpartum"],
    "Behavioral\nhealth": ["FUM30", "AMM acute", "AMM continuation"]
}

# Data for all measures
data = {
    "PCR": {"SDoH": 1.8, "non-SDoH": 1.6, "lower SDoH": 0.9, "upper SDoH": 2.2,
            "lower non-SDoH": 0.6, "upper non-SDoH": 2.1,
            "Random Selection": 50.0, "lower Random Selection": 49.0, "upper Random Selection": 51.0},
    "SPC adherence": {"SDoH": 39.2, "non-SDoH": 33.8, "lower SDoH": 36.9, "upper SDoH": 41.1,
                      "lower non-SDoH": 29.9, "upper non-SDoH": 36.1,
                      "Random Selection": 50.8, "lower Random Selection": 49.7, "upper Random Selection": 51.9},
    "SPD adherence": {"SDoH": 49.3, "non-SDoH": 33.4, "lower SDoH": 48.6, "upper SDoH": 50.1,
                      "lower non-SDoH": 31.3, "upper non-SDoH": 34.6,
                      "Random Selection": 51.0, "lower Random Selection": 49.9, "upper Random Selection": 52.1},
    "SPC utilization": {"SDoH": 84.5, "non-SDoH": 84.1, "lower SDoH": 83.0, "upper SDoH": 85.7,
                        "lower non-SDoH": 82.4, "upper non-SDoH": 85.4,
                        "Random Selection": 50.8, "lower Random Selection": 49.7, "upper Random Selection": 51.9},
    "SPD utilization": {"SDoH": 74.8, "non-SDoH": 79.2, "lower SDoH": 72.7, "upper SDoH": 77.2,
                        "lower non-SDoH": 76.0, "upper non-SDoH": 81.7,
                        "Random Selection": 50.5, "lower Random Selection": 49.4, "upper Random Selection": 51.6},
    "PBH": {"SDoH": 49.8, "non-SDoH": 42.9, "lower SDoH": 48.2, "upper SDoH": 51.7,
            "lower non-SDoH": 41.0, "upper non-SDoH": 45.6,
            "Random Selection": 50.0, "lower Random Selection": 49.0, "upper Random Selection": 51.0},
    "LBP": {"SDoH": 16.7, "non-SDoH": 12.8, "lower SDoH": 15.0, "upper SDoH": 17.6,
            "lower non-SDoH": 11.0, "upper non-SDoH": 14.3,
            "Random Selection": 50.9, "lower Random Selection": 49.8, "upper Random Selection": 52.0},
    "WCV": {"SDoH": 88.3, "non-SDoH": 79.1, "lower SDoH": 81.5, "upper SDoH": 91.5,
            "lower non-SDoH": 77.7, "upper non-SDoH": 80.3,
            "Random Selection": 50.6, "lower Random Selection": 49.5, "upper Random Selection": 51.7},
    "Prenatal": {"SDoH": 55.9, "non-SDoH": 43.5, "lower SDoH": 54.2, "upper SDoH": 57.6,
                 "lower non-SDoH": 43.4, "upper non-SDoH": 47.1,
                 "Random Selection": 50.0, "lower Random Selection": 49.0, "upper Random Selection": 51.0},
    "Postpartum": {"SDoH": 70.5, "non-SDoH": 65.4, "lower SDoH": 69.3, "upper SDoH": 71.9,
                   "lower non-SDoH": 63.8, "upper non-SDoH": 66.9,
                   "Random Selection": 50.6, "lower Random Selection": 49.5, "upper Random Selection": 51.7},
    "FUM30": {"SDoH": 87.4, "non-SDoH": 77.6, "lower SDoH": 86.1, "upper SDoH": 88.9,
              "lower non-SDoH": 76.0, "upper non-SDoH": 79.3,
              "Random Selection": 50.0, "lower Random Selection": 49.0, "upper Random Selection": 51.0},
    "AMM acute": {"SDoH": 77.6, "non-SDoH": 76.3, "lower SDoH": 76.1, "upper SDoH": 80.1,
                  "lower non-SDoH": 74.2, "upper non-SDoH": 78.9,
                  "Random Selection": 51.0, "lower Random Selection": 49.9, "upper Random Selection": 52.1},
    "AMM continuation": {"SDoH": 89.1, "non-SDoH": 86.3, "lower SDoH": 88.7, "upper SDoH": 89.5,
                         "lower non-SDoH": 85.8, "upper non-SDoH": 87.3,
                         "Random Selection": 50.5, "lower Random Selection": 49.2, "upper Random Selection": 51.8}
}


# COMMAND ----------

import matplotlib.pyplot as plt

# Order measures by SDoH sensitivity (descending)
ordered_measures = sorted(data.items(), key=lambda x: x[1]["SDoH"], reverse=False)

# Plot setup
fig, ax = plt.subplots(figsize=(8, 8))
current_y = 0
y_ticks = []

# Loop through sorted measures and plot
for measure, values in ordered_measures:
    # Determine the category for coloring
    category = next((cat for cat, measures in categories.items() if measure in measures), None)
    background_color = group_colors.get(category, "white")  # Use "white" for no shading

    # Highlight the row with category color (no overlap)
    ax.axhspan(
        current_y - 0.5, current_y + 0.5, facecolor=background_color, alpha=0.2, zorder=-1
    )
    
    # Add y-tick for the measure
    y_ticks.append((current_y, measure))
    
    # Plot SDoH
    ax.errorbar(
        x=[values["SDoH"]],
        y=current_y,
        xerr=[[values["SDoH"] - values["lower SDoH"]], [values["upper SDoH"] - values["SDoH"]]],
        fmt="o",
        color=point_colors["SDoH"],
        label="SDoH" if current_y == 0 else "",
    )

    # Plot non-SDoH
    ax.errorbar(
        x=[values["non-SDoH"]],
        y=current_y,
        xerr=[[values["non-SDoH"] - values["lower non-SDoH"]], [values["upper non-SDoH"] - values["non-SDoH"]]],
        fmt="o",
        color=point_colors["non-SDoH"],
        label="non-SDoH" if current_y == 0 else "",
    )

    # Plot Random Selection
    ax.errorbar(
        x=[values["Random Selection"]],
        y=current_y,
        xerr=[[values["Random Selection"] - values["lower Random Selection"]],
              [values["upper Random Selection"] - values["Random Selection"]]],
        fmt="o",
        color=point_colors["Random Selection"],
        label="Random selection" if current_y == 0 else "",
    )

    current_y += 1  # Adjust spacing between rows

# Add legends
point_legend = ax.legend(fontsize=10, loc="upper left", title="Outreach model")  # Rename top legend
handles = [plt.Line2D([0], [0], color=color, lw=4, label=group) for group, color in group_colors.items()]
group_legend = ax.legend(
    handles=handles,
    fontsize=10,
    loc="upper left",
    bbox_to_anchor=(0.0, 0.70),  # Add distance between legends
    title="Groups",
)
ax.add_artist(point_legend)

# Top legend for Outreach model

point_legend = ax.legend(
    fontsize=10,
    loc="upper left",
    title="Outreach model",
    bbox_to_anchor=(0.0, 1.0)  # Keep the top legend at the top
)

# Bottom legend for Groups
handles = [
    plt.Line2D([0], [0], color=color, lw=4, label=group) for group, color in group_colors.items()
]
group_legend = ax.legend(
    handles=handles,
    fontsize=10,
    loc="upper left",
    bbox_to_anchor=(0.0, 0.85),  # Move closer to the top legend
    title="Groups"
)

# Add the first legend back to ensure both are displayed
ax.add_artist(point_legend)

# Configure y-axis
ax.set_yticks([y for y, _ in y_ticks])
ax.set_yticklabels([measure.replace(" ", "\n") for _, measure in y_ticks], fontsize=10)

# Configure x-axis
ax.set_xlabel("Performance metric value (0-100%)", fontsize=12)
ax.set_xlim(0, 100)
ax.set_xticks(range(0, 101, 20))

# Add grid and title
ax.grid(True, axis="x", linestyle="--", alpha=0.6)
ax.set_title("Figure 1(a): Sensitivity (ordered by decreasing SDoH sensitivity)", fontsize=12)

# Save to in-memory buffer as PNG with high DPI
buf = io.BytesIO()
plt.savefig(buf, format='png', dpi=1000, bbox_inches='tight')
buf.seek(0)

# Display the image inline
img = Image.open(buf)
display(img)

# COMMAND ----------

plt.tight_layout()
plt.savefig("/dbfs/tmp/figure1a_sensitivity.pdf", format="pdf", bbox_inches="tight")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC specificity

# COMMAND ----------

# Define group colors
group_colors = {
    "Care\ncoordination": "purple",
    "Chronic disease\nmanagement": "green",
    "Unnecessary care": "brown",
    "Maternal and\nchild health": "cyan",  # Split into two lines
    "Behavioral\nhealth": "pink"
}

# Define point colors for SDoH, non-SDoH, and Random Selection
point_colors = {"SDoH": "blue", "non-SDoH": "orange", "Random Selection": "red"}

# Categories and measures (corrected PBH group)
categories = {
    "Care\ncoordination": ["PCR"],
    "Chronic disease\nmanagement": ["SPC adherence", "SPD adherence", "SPC utilization", "SPD utilization", "PBH"],
    "Unnecessary care": ["LBP"],
    "Maternal and\nchild health": ["WCV", "Prenatal", "Postpartum"],
    "Behavioral\nhealth": ["FUM30", "AMM acute", "AMM continuation"]
}

# Data for all measures
data = {
    "PCR": {"SDoH": 10.5, "non-SDoH": 9.6, "lower SDoH": 6.5, "upper SDoH": 12.5, 
            "lower non-SDoH": 5.3, "upper non-SDoH": 11.7, 
            "Random Selection": 50.0, "lower Random Selection": 49.0, "upper Random Selection": 51.0},
    "WCV": {"SDoH": 26.6, "non-SDoH": 25.0, "lower SDoH": 24.1, "upper SDoH": 28.7, 
            "lower non-SDoH": 22.3, "upper non-SDoH": 27.1, 
            "Random Selection": 50.5, "lower Random Selection": 49.4, "upper Random Selection": 51.6},
    "LBP": {"SDoH": 34.6, "non-SDoH": 29.7, "lower SDoH": 32.5, "upper SDoH": 35.8, 
            "lower non-SDoH": 27.5, "upper non-SDoH": 31.9, 
            "Random Selection": 50.3, "lower Random Selection": 49.2, "upper Random Selection": 51.4},
    "SPC adherence": {"SDoH": 55.1, "non-SDoH": 51.2, "lower SDoH": 53.2, "upper SDoH": 56.6, 
                      "lower non-SDoH": 47.6, "upper non-SDoH": 52.9, 
                      "Random Selection": 50.7, "lower Random Selection": 49.6, "upper Random Selection": 51.8},
    "SPD adherence": {"SDoH": 57.4, "non-SDoH": 51.9, "lower SDoH": 55.7, "upper SDoH": 58.1, 
                      "lower non-SDoH": 49.5, "upper non-SDoH": 53.1, 
                      "Random Selection": 50.9, "lower Random Selection": 49.8, "upper Random Selection": 52.0},
    "FUM30": {"SDoH": 60.0, "non-SDoH": 47.6, "lower SDoH": 59.3, "upper SDoH": 61.5, 
              "lower non-SDoH": 45.5, "upper non-SDoH": 52.2, 
              "Random Selection": 50.4, "lower Random Selection": 49.3, "upper Random Selection": 51.5},
    "AMM continuation": {"SDoH": 61.6, "non-SDoH": 55.0, "lower SDoH": 58.4, "upper SDoH": 63.7, 
                         "lower non-SDoH": 51.5, "upper non-SDoH": 56.8, 
                         "Random Selection": 50.8, "lower Random Selection": 49.7, "upper Random Selection": 51.9},
    "Postpartum": {"SDoH": 66.3, "non-SDoH": 60.7, "lower SDoH": 65.5, "upper SDoH": 68.1, 
                   "lower non-SDoH": 59.7, "upper non-SDoH": 62.0, 
                   "Random Selection": 50.6, "lower Random Selection": 49.5, "upper Random Selection": 51.7},
    "PBH": {"SDoH": 66.4, "non-SDoH": 60.7, "lower SDoH": 64.5, "upper SDoH": 68.5, 
            "lower non-SDoH": 59.7, "upper non-SDoH": 63.8, 
            "Random Selection": 50.5, "lower Random Selection": 49.4, "upper Random Selection": 51.6},
    "Prenatal": {"SDoH": 66.5, "non-SDoH": 56.2, "lower SDoH": 65.4, "upper SDoH": 67.9, 
                 "lower non-SDoH": 55.0, "upper non-SDoH": 58.4, 
                 "Random Selection": 50.4, "lower Random Selection": 49.3, "upper Random Selection": 51.5},
    "AMM acute": {"SDoH": 70.4, "non-SDoH": 71.5, "lower SDoH": 69.6, "upper SDoH": 75.9, 
                  "lower non-SDoH": 69.5, "upper non-SDoH": 72.7, 
                  "Random Selection": 50.7, "lower Random Selection": 49.6, "upper Random Selection": 51.8},
    "SPD utilization": {"SDoH": 74.1, "non-SDoH": 65.8, "lower SDoH": 72.7, "upper SDoH": 76.0, 
                        "lower non-SDoH": 63.7, "upper non-SDoH": 67.4, 
                        "Random Selection": 50.5, "lower Random Selection": 49.4, "upper Random Selection": 51.6},
    "SPC utilization": {"SDoH": 82.1, "non-SDoH": 81.2, "lower SDoH": 80.6, "upper SDoH": 82.8, 
                        "lower non-SDoH": 79.5, "upper non-SDoH": 82.3, 
                        "Random Selection": 50.8, "lower Random Selection": 49.7, "upper Random Selection": 51.9},
}

# COMMAND ----------

import matplotlib.pyplot as plt

# Order measures by SDoH sensitivity (descending) EXACTLY as per the provided figure (flipped for top-to-bottom order)
ordered_measures = [
    "PCR", "LBP", "SPC adherence", "SPD adherence", "PBH", "Prenatal", 
    "Postpartum", "SPD utilization", "AMM acute", "SPC utilization", 
    "FUM30", "WCV", "AMM continuation"
]

# Plot setup
fig, ax = plt.subplots(figsize=(8, 8))
current_y = 0
y_ticks = []

# Loop through ordered measures and plot
for measure in ordered_measures:
    values = data[measure]
    
    # Determine the category for coloring
    category = next((cat for cat, measures in categories.items() if measure in measures), None)
    background_color = group_colors.get(category, "white")  # Default to "white" if no color
    
    # Highlight the row with the category color
    ax.axhspan(
        current_y - 0.5, current_y + 0.5, facecolor=background_color, alpha=0.2, zorder=-1
    )
    
    # Add y-tick for the measure
    y_ticks.append((current_y, measure))
    
    # Plot SDoH
    ax.errorbar(
        x=[values["SDoH"]],
        y=current_y,
        xerr=[[values["SDoH"] - values["lower SDoH"]], [values["upper SDoH"] - values["SDoH"]]],
        fmt="o",
        color=point_colors["SDoH"],
    )

    # Plot non-SDoH
    ax.errorbar(
        x=[values["non-SDoH"]],
        y=current_y,
        xerr=[[values["non-SDoH"] - values["lower non-SDoH"]], [values["upper non-SDoH"] - values["non-SDoH"]]],
        fmt="o",
        color=point_colors["non-SDoH"],
    )

    # Plot Random Selection
    ax.errorbar(
        x=[values["Random Selection"]],
        y=current_y,
        xerr=[[values["Random Selection"] - values["lower Random Selection"]],
              [values["upper Random Selection"] - values["Random Selection"]]],
        fmt="o",
        color=point_colors["Random Selection"],
    )

    current_y += 1  # Move to the next row

# Configure y-axis
ax.set_yticks([y for y, _ in y_ticks])
ax.set_yticklabels([measure.replace(" ", "\n") for _, measure in y_ticks], fontsize=10)

# Configure x-axis
ax.set_xlabel("Performance metric value (0-100%)", fontsize=12)
ax.set_xlim(0, 100)
ax.set_xticks(range(0, 101, 20))

# Add grid and title
ax.grid(True, axis="x", linestyle="--", alpha=0.6)
ax.set_title("Panel 1(b): Specificity (ordered by decreasing SDoH sensitivity)", fontsize=12)

# Save to in-memory buffer as PNG with high DPI
buf = io.BytesIO()
plt.savefig(buf, format='png', dpi=1000, bbox_inches='tight')
buf.seek(0)

# Display the image inline
img = Image.open(buf)
display(img)


# COMMAND ----------

# MAGIC %md
# MAGIC accuracy

# COMMAND ----------

# Define group colors
group_colors = {
    "Care\ncoordination": "purple",
    "Chronic disease\nmanagement": "green",
    "Unnecessary care": "brown",
    "Maternal and\nchild health": "cyan",  # Split into two lines
    "Behavioral\nhealth": "pink"
}

# Define point colors for SDoH, non-SDoH, and Random Selection
point_colors = {"SDoH": "blue", "non-SDoH": "orange", "Random Selection": "red"}

# Categories and measures (corrected PBH group)
categories = {
    "Care\ncoordination": ["PCR"],
    "Chronic disease\nmanagement": ["SPC adherence", "SPD adherence", "SPC utilization", "SPD utilization", "PBH"],
    "Unnecessary care": ["LBP"],
    "Maternal and\nchild health": ["WCV", "Prenatal", "Postpartum"],
    "Behavioral\nhealth": ["FUM30", "AMM acute", "AMM continuation"]
}

data = {
    "Prenatal": {
        "SDoH": 87.9, "lower SDoH": 85.5, "upper SDoH": 88.5,
        "non-SDoH": 84.6, "lower non-SDoH": 83.0, "upper non-SDoH": 85.5,
        "Random Selection": 50.0, "lower Random Selection": 49.4, "upper Random Selection": 50.5
    },
    "Postpartum": {
        "SDoH": 83.2, "lower SDoH": 81.0, "upper SDoH": 84.2,
        "non-SDoH": 80.2, "lower non-SDoH": 78.0, "upper non-SDoH": 81.0,
        "Random Selection": 50.0, "lower Random Selection": 49.3, "upper Random Selection": 50.6
    },
    "LBP": {
        "SDoH": 76.0, "lower SDoH": 75.0, "upper SDoH": 77.0,
        "non-SDoH": 72.5, "lower non-SDoH": 71.0, "upper non-SDoH": 73.0,
        "Random Selection": 50.0, "lower Random Selection": 49.2, "upper Random Selection": 50.6
    },
    "PCR": {
        "SDoH": 80.3, "lower SDoH": 79.5, "upper SDoH": 81.1,
        "non-SDoH": 77.6, "lower non-SDoH": 76.5, "upper non-SDoH": 78.5,
        "Random Selection": 50.0, "lower Random Selection": 49.5, "upper Random Selection": 50.5
    },
    "PBH": {
        "SDoH": 81.8, "lower SDoH": 81.0, "upper SDoH": 82.6,
        "non-SDoH": 80.9, "lower non-SDoH": 79.8, "upper non-SDoH": 81.5,
        "Random Selection": 50.0, "lower Random Selection": 49.5, "upper Random Selection": 50.5
    },
    "FUM30": {
        "SDoH": 81.8, "lower SDoH": 80.8, "upper SDoH": 82.8,
        "non-SDoH": 77.5, "lower non-SDoH": 76.5, "upper non-SDoH": 78.0,
        "Random Selection": 50.0, "lower Random Selection": 49.5, "upper Random Selection": 50.5
    },
    "AMM acute": {
        "SDoH": 81.8, "lower SDoH": 80.8, "upper SDoH": 82.8,
        "non-SDoH": 80.8, "lower non-SDoH": 79.8, "upper non-SDoH": 81.8,
        "Random Selection": 50.0, "lower Random Selection": 49.5, "upper Random Selection": 50.5
    },
    "AMM continuation": {
        "SDoH": 81.8, "lower SDoH": 80.8, "upper SDoH": 82.8,
        "non-SDoH": 80.8, "lower non-SDoH": 79.8, "upper non-SDoH": 81.8,
        "Random Selection": 50.0, "lower Random Selection": 49.5, "upper Random Selection": 50.5
    },
    "SPC utilization": {
        "SDoH": 86.7, "lower SDoH": 85.9, "upper SDoH": 87.5,
        "non-SDoH": 85.1, "lower non-SDoH": 84.0, "upper non-SDoH": 86.0,
        "Random Selection": 50.0, "lower Random Selection": 49.5, "upper Random Selection": 50.5
    },
    "SPD adherence": {
        "SDoH": 86.7, "lower SDoH": 85.9, "upper SDoH": 87.5,
        "non-SDoH": 85.1, "lower non-SDoH": 84.0, "upper non-SDoH": 86.0,
        "Random Selection": 50.0, "lower Random Selection": 49.5, "upper Random Selection": 50.5
    },
    "SPC adherence": {
        "SDoH": 86.7, "lower SDoH": 85.9, "upper SDoH": 87.5,
        "non-SDoH": 85.1, "lower non-SDoH": 84.0, "upper non-SDoH": 86.0,
        "Random Selection": 50.0, "lower Random Selection": 49.5, "upper Random Selection": 50.5
    },
    "SPD utilization": {
        "SDoH": 86.7, "lower SDoH": 85.9, "upper SDoH": 87.5,
        "non-SDoH": 85.1, "lower non-SDoH": 84.0, "upper non-SDoH": 86.0,
        "Random Selection": 50.0, "lower Random Selection": 49.5, "upper Random Selection": 50.5
    },
    "WCV": {
        "SDoH": 86.7, "lower SDoH": 85.9, "upper SDoH": 87.5,
        "non-SDoH": 85.1, "lower non-SDoH": 84.0, "upper non-SDoH": 86.0,
        "Random Selection": 50.0, "lower Random Selection": 49.5, "upper Random Selection": 50.5
    },
}


# COMMAND ----------

import matplotlib.pyplot as plt

# Order measures by SDoH sensitivity (descending) EXACTLY as per the provided figure (flipped for top-to-bottom order)
ordered_measures = [
    "PCR", "LBP", "SPC adherence", "SPD adherence", "PBH", "Prenatal", 
    "Postpartum", "SPD utilization", "AMM acute", "SPC utilization", 
    "FUM30", "WCV", "AMM continuation"
]

# Plot setup
fig, ax = plt.subplots(figsize=(8, 8))
current_y = 0
y_ticks = []

# Loop through ordered measures and plot
for measure in ordered_measures:
    values = data[measure]
    
    # Determine the category for coloring
    category = next((cat for cat, measures in categories.items() if measure in measures), None)
    background_color = group_colors.get(category, "white")  # Default to "white" if no color
    
    # Highlight the row with the category color
    ax.axhspan(
        current_y - 0.5, current_y + 0.5, facecolor=background_color, alpha=0.2, zorder=-1
    )
    
    # Add y-tick for the measure
    y_ticks.append((current_y, measure))
    
    # Plot SDoH
    ax.errorbar(
        x=[values["SDoH"]],
        y=current_y,
        xerr=[[values["SDoH"] - values["lower SDoH"]], [values["upper SDoH"] - values["SDoH"]]],
        fmt="o",
        color=point_colors["SDoH"],
    )

    # Plot non-SDoH
    ax.errorbar(
        x=[values["non-SDoH"]],
        y=current_y,
        xerr=[[values["non-SDoH"] - values["lower non-SDoH"]], [values["upper non-SDoH"] - values["non-SDoH"]]],
        fmt="o",
        color=point_colors["non-SDoH"],
    )

    # Plot Random Selection
    ax.errorbar(
        x=[values["Random Selection"]],
        y=current_y,
        xerr=[[values["Random Selection"] - values["lower Random Selection"]],
              [values["upper Random Selection"] - values["Random Selection"]]],
        fmt="o",
        color=point_colors["Random Selection"],
    )

    current_y += 1  # Move to the next row

# Configure y-axis
ax.set_yticks([y for y, _ in y_ticks])
ax.set_yticklabels([measure.replace(" ", "\n") for _, measure in y_ticks], fontsize=10)

# Configure x-axis
ax.set_xlabel("Performance metric value (0-100%)", fontsize=12)
ax.set_xlim(0, 100)
ax.set_xticks(range(0, 101, 20))

# Add grid and title
ax.grid(True, axis="x", linestyle="--", alpha=0.6)
ax.set_title("Panel 1(c): Accuracy (ordered by decreasing SDoH sensitivity)", fontsize=12)

# Save to in-memory buffer as PNG with high DPI
buf = io.BytesIO()
plt.savefig(buf, format='png', dpi=1000, bbox_inches='tight')
buf.seek(0)

# Display the image inline
img = Image.open(buf)
display(img)

# COMMAND ----------

