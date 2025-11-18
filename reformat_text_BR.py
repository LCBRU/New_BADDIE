def reformat_clinical_text(raw_text):
    # Replace <BR> tags with newline characters
    text = raw_text.replace("<BR>", "\n")

    # Split into lines and remove empty lines
    lines = [line.strip() for line in text.split('\n') if line.strip()]

    # Initialize formatted output
    formatted = []

    # Define simple rules for formatting
    for line in lines:
        if line.lower().startswith("clinical history"):
            formatted.append("**Clinical History:**")
        elif line.lower().startswith("unsupervised study"):
            formatted.append("**Study Details:**")
        elif line.lower().startswith("findings"):
            formatted.append("---\n### Findings")
        elif line.lower().startswith("left ventricle"):
            formatted.append("#### Left Ventricle (LV):")
        elif line.lower().startswith("lv volume analysis"):
            formatted.append("**LV Volume Analysis:**")
        elif line.lower().startswith("right ventricle"):
            formatted.append("#### Right Ventricle (RV):")
        elif line.lower().startswith("rv volume analysis"):
            formatted.append("**RV Volume Analysis:**")
        elif line.lower().startswith("atria"):
            formatted.append("#### Atria:")
        elif line.lower().startswith("valves"):
            formatted.append("#### Valves:")
        elif line.lower().startswith("great arteries"):
            formatted.append("#### Great Arteries:")
        elif line.lower().startswith("pericardium"):
            formatted.append("#### Pericardium:")
        elif line.lower().startswith("gadolinium study"):
            formatted.append("#### Gadolinium Study:")
        elif line.lower().startswith("summary"):
            formatted.append("---\n### Summary")
        elif line.lower().startswith("dr "):
            formatted.append("---\n**Reported by:**")
        else:
            formatted.append(f"- {line}")

    return '\n'.join(formatted)

# Example usage
clinical_text = """<BR>Clinical History : Spontaneous dissection of left main stem<BR>coronary artery<BR>Unsupervised study <BR>..."""  # Replace with full text
print(reformat_clinical_text(clinical_text))


def remove_section(text_input, section_heading):
    lines = text_input.split('\n')
    output_lines = []
    skip = False

    for line in lines:
        # Check if the line starts the section to remove
        if section_heading.lower() in line.lower():
            skip = True
            continue
        # Stop skipping if a new heading starts (e.g., lines starting with '###' or '####')
        if skip and (line.startswith('###') or line.startswith('####') or line.startswith('**')):
            skip = False
        if not skip:
            output_lines.append(line)

    return '\n'.join(output_lines)