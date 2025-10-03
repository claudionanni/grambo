#!/usr/bin/env python3
"""Create an annotated version of the Grambo dashboard screenshot"""

from PIL import Image, ImageDraw, ImageFont
import os

# Load the original screenshot
img_path = "Grambo_v3-alpha-001.png"
img = Image.open(img_path)
width, height = img.size

print(f"Image dimensions: {width}x{height}")

# Create a drawing context
draw = ImageDraw.Draw(img)

# Try to load a nice font, fallback to default
try:
    # Try different font sizes
    title_font = ImageFont.truetype("/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf", 28)
    label_font = ImageFont.truetype("/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf", 22)
    desc_font = ImageFont.truetype("/usr/share/fonts/dejavu/DejaVuSans.ttf", 18)
except:
    try:
        title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
        label_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 22)
        desc_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 18)
    except:
        title_font = label_font = desc_font = ImageFont.load_default()

# Define colors
highlight_color = (255, 200, 0, 200)  # Orange/yellow with transparency
box_color = (255, 255, 255, 230)  # White with slight transparency
text_color = (0, 0, 0)  # Black text
accent_color = (255, 87, 34)  # Orange accent
line_color = (255, 200, 0, 255)  # Bright yellow for lines

# Define annotation areas (approximate based on typical grav layout)
# Format: (x, y, width, height, number, title, description)
annotations = [
    # Timeline area at top
    (50, 50, width-100, 100, "1", "Dual Timeline Navigation",
     "Frame-by-frame navigation + Natural timestamp-based timeline with SST markers"),
    
    # Left side - main content area
    (50, 200, width//2-100, 250, "2", "Current SST Session Details",
     "Active SST operation: joiner/donor nodes, method, status, timestamps"),
    
    # Node states section
    (50, 500, width//2-100, 200, "3", "Node States & Views",
     "Real-time cluster topology: node roles, states (SYNCED/JOINER/DONOR)"),
    
    # Right side - raw logs
    (width//2+50, 200, width//2-100, height-250, "4", "Inline Raw Error Log",
     "Contextual log lines around current frame timestamp with highlighting"),
    
    # Quorum/cluster info (if visible)
    (50, 750, width//2-100, 150, "5", "Cluster & Quorum State",
     "Cluster configuration, quorum status, primary component indicator"),
]

# Draw numbered callouts with lines
for idx, (x, y, w, h, num, title, desc) in enumerate(annotations):
    # Draw a semi-transparent highlight box
    draw.rectangle([x, y, x+w, y+h], outline=highlight_color, width=4)
    
    # Draw number circle
    circle_x = x - 30
    circle_y = y + h//2 - 20
    circle_radius = 25
    
    # Circle background
    draw.ellipse([circle_x-circle_radius, circle_y-circle_radius,
                  circle_x+circle_radius, circle_y+circle_radius],
                 fill=accent_color, outline=(255, 255, 255), width=3)
    
    # Number text
    num_bbox = draw.textbbox((0, 0), num, font=label_font)
    num_width = num_bbox[2] - num_bbox[0]
    num_height = num_bbox[3] - num_bbox[1]
    draw.text((circle_x - num_width//2, circle_y - num_height//2), 
              num, fill=(255, 255, 255), font=label_font)
    
    # Draw connecting line from circle to box
    draw.line([circle_x + circle_radius, circle_y, x, y + h//2], 
              fill=line_color, width=3)

# Add title at the top
title_text = "Grambo v3-alpha Dashboard Overview"
title_bbox = draw.textbbox((0, 0), title_text, font=title_font)
title_width = title_bbox[2] - title_bbox[0]

# Title background
title_x = (width - title_width) // 2 - 20
title_y = 5
draw.rectangle([title_x, title_y, title_x + title_width + 40, title_y + 45],
               fill=box_color)
draw.text((title_x + 20, title_y + 8), title_text, fill=text_color, font=title_font)

# Add legend at bottom
legend_y = height - 180
legend_items = [
    ("1", "Dual Timeline: Frame navigation + Natural timestamp timeline with SST markers"),
    ("2", "SST Session: Current state transfer operation details (joiner ← donor)"),
    ("3", "Node States: Cluster topology with node roles and states (SYNCED/JOINER/DONOR)"),
    ("4", "Raw Logs: Inline error log with contextual lines around current timestamp"),
    ("5", "Cluster Info: Quorum state, cluster configuration, primary component status"),
]

# Legend background
draw.rectangle([30, legend_y - 10, width - 30, height - 20],
               fill=box_color, outline=accent_color, width=2)

y_offset = legend_y
for num, text in legend_items:
    # Number circle (small)
    circle_x = 50
    circle_y = y_offset + 8
    circle_r = 12
    draw.ellipse([circle_x-circle_r, circle_y-circle_r,
                  circle_x+circle_r, circle_y+circle_r],
                 fill=accent_color, outline=(255, 255, 255), width=2)
    
    num_bbox = draw.textbbox((0, 0), num, font=desc_font)
    num_width = num_bbox[2] - num_bbox[0]
    num_height = num_bbox[3] - num_bbox[1]
    draw.text((circle_x - num_width//2, circle_y - num_height//2), 
              num, fill=(255, 255, 255), font=desc_font)
    
    # Text
    draw.text((75, y_offset), text, fill=text_color, font=desc_font)
    y_offset += 30

# Save the annotated version
output_path = "Grambo_v3-alpha-001_annotated.png"
img.save(output_path, quality=95)
print(f"Annotated screenshot saved to: {output_path}")
print(f"File size: {os.path.getsize(output_path) / 1024:.1f} KB")
