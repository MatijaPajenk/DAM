import csv
import matplotlib.pyplot as plt

x = []
y = []
z = []

# Replace with your CSV file path
csv_file = "/home/mic/Code/DAM/data/file_mini.csv"

with open(csv_file, "r") as f:
    reader = csv.reader(f)
    header = next(reader)  # skip header
    
    for row in reader:
        x.append(float(row[0]))
        y.append(float(row[1]))
        z.append(float(row[2]))

# Create scatter plot
plt.figure()
scatter = plt.scatter(x, y, c=z, cmap='viridis')

# Add colorbar to show z scale
cbar = plt.colorbar(scatter)
cbar.set_label("Z value")

plt.xlabel("X")
plt.ylabel("Y")
plt.title("2D Visualization of CSV Data (Color = Z)")

plt.show()