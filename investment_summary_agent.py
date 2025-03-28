import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from crewai import Agent, Task, Crew, Process
from langchain_openai import ChatOpenAI
from typing import Dict, Any
import json
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Check for OpenAI API Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.error("⚠️ OPENAI_API_KEY environment variable is not set!")
    raise ValueError("OpenAI API key is required")

# PostgreSQL Configuration
db_config = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Database connection function
def get_db_connection():
    """Create a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"],
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def get_pitch_deck_data(pitch_id: int) -> Dict[str, Any]:
    """
    Fetch all relevant data for a pitch deck from the database
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query to get all relevant data columns
        query = """
        SELECT 
            i."investorSettingsAgent",
            p.ExternalValidationAgent AS externalvalidationagent,
            p.finalPitchDeckAgent AS finalpitchdeckagent,
            p.extracted_text AS pitch_deck_data,
            p."ThesisSettingsAgent"
        FROM pitch_decks p 
        JOIN investor_profiles i 
        ON i.investor_id = p.investor_id
        WHERE pitch_id = %s
        """
        
        cursor.execute(query, (pitch_id,))
        result = cursor.fetchone()
        
        # Log the raw result for debugging
        if result:
            logger.info(f"Raw database result keys: {result.keys()}")
            for key in result.keys():
                val_preview = str(result[key])[:50] + "..." if result[key] and len(str(result[key])) > 50 else str(result[key])
                logger.info(f"Column '{key}': {val_preview}")
        
        cursor.close()
        conn.close()

        if not result:
            logger.warning(f"No data found for pitch ID: {pitch_id}")
            return None
            
        return dict(result)
            
    except Exception as e:
        logger.error(f"❌ Database Error: {str(e)}", exc_info=True)
        return None

def generate_executive_summary(pitch_id: int) -> Dict[str, Any]:
    """
    Generate a comprehensive executive summary for a pitch deck with signal strength, 
    innovation index, thesis fit score, and final recommendation
    """
    # Check for OpenAI API Key before proceeding
    if not OPENAI_API_KEY:
        return {"error": "OpenAI API key is not configured"}
    
    logger.info(f"🔥 Starting executive summary generation for pitch ID: {pitch_id}")

    # Fetch data from database
    data = get_pitch_deck_data(pitch_id)
    
    if not data:
        return {"error": f"Could not retrieve data for pitch ID: {pitch_id}"}
    
    # Log summary of available data and check for empty values
    logger.info(f"Data retrieved for pitch ID {pitch_id}:")
    missing_fields = []
    for key, value in data.items():
        if value:
            sample = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
            logger.info(f"- {key}: {sample}")
        else:
            logger.info(f"- {key}: None or empty")
            missing_fields.append(key)
    
    if missing_fields:
        logger.warning(f"Missing data for fields: {', '.join(missing_fields)}")
    
    # Initialize the CrewAI agents
    llm = ChatOpenAI(
        api_key=OPENAI_API_KEY,
        temperature=0.2,
        model="gpt-4o"
    )
    
    # Create the analyst agent
    analyst = Agent(
        role="Executive Summary Analyst",
        goal="Generate comprehensive executive summary with assessment sections and final recommendation",
        backstory="""You are an expert venture capital analyst specializing in creating 
        executive summaries and evaluating startup opportunities. Your analyses are always 
        data-driven, concise, and highlight the key points investors need to know. You provide
        actionable recommendations that help investors make informed decisions.""",
        verbose=True,
        llm=llm
    )
    
    # Process the data - Match variable names to SQL column names and ensure we're using exactly the right case
    # Check all possible variations of the column names
    extracted_text = data.get('pitch_deck_data', '') or data.get('extracted_text', '')
    
    # For investor settings, check multiple possible column names
    investor_settings = (data.get('investorSettingsAgent', '') or 
                         data.get('InvestorSettingsAgent', '') or 
                         data.get('investor_settings_agent', ''))
    
    # For external validation, check multiple possible column names
    external_validation = (data.get('ExternalValidationAgent', '') or 
                          data.get('externalvalidationagent', '') or 
                          data.get('external_validation_agent', ''))
    
    # For final pitch analysis, check multiple possible column names
    final_pitch_analysis = (data.get('finalpitchdeckagent', '') or 
                           data.get('FinalPitchDeckAgent', '') or 
                           data.get('final_pitch_deck_agent', ''))
    
    # For thesis settings, check multiple possible column names
    thesis_settings = (data.get('ThesisSettingsAgent', '') or 
                      data.get('thesisSettingsAgent', '') or 
                      data.get('thesis_settings_agent', ''))
    
    # Check if we have enough data to proceed
    if not extracted_text:
        logger.error("No pitch deck text available - cannot generate analysis")
        return {"error": "No pitch deck text available for analysis"}
    
    # Format data for the prompt with additional information when data is missing
    def format_data_section(title, content):
        if not content:
            return f"### {title}\nNo data available. Proceed with analysis using other available data."
        return f"### {title}\n{content}"
    
    formatted_data = "\n\n".join([
        format_data_section("PITCH DECK TEXT", extracted_text),
        format_data_section("INVESTOR SETTINGS", investor_settings),
        format_data_section("EXTERNAL VALIDATION", external_validation),
        format_data_section("FINAL PITCH ANALYSIS", final_pitch_analysis),
        format_data_section("THESIS SETTINGS", thesis_settings)
    ])
    
    # Create the analysis task with instructions for handling missing data
    analysis_task = Task(
        description=f"""
        Your task is to generate a comprehensive Executive Summary for a startup based on its pitch deck
        and related data. Include Signal Strength assessment, Innovation Index assessment, 
        Market Pulse assessment, Thesis Fit Score assessment, and a Final Recommendation.

        ANALYZE THE FOLLOWING DATA:

        {formatted_data}

        IMPORTANT NOTE ON MISSING DATA:
        If any sections above show "No data available", you should:
        1. Focus primarily on the available data (especially the pitch deck text)
        2. Make reasonable inferences based on industry knowledge
        3. Note in your analysis when you're working with limited information
        4. Still provide complete assessments for all sections requested

        REQUIREMENTS:
        1. First, identify the startup name from the pitch deck text.
        
        2. Create a comprehensive Signal Strength assessment that includes:
        - Problem Overview: Summarize the core problem the startup addresses
        - Validation & Supporting Data: Evidence from available data or industry knowledge
        - External Research: Insights based on available information
        - Data Matching: Assessment of how well the pitch deck claims match with external validation data
        - Conclusion: Overall conclusion about the signal strength and credibility of the startup
        
        3. Create a comprehensive Innovation Index assessment that includes:
        - Solution Overview: Summary of the startup's solution approach
        - Technology & Differentiation: Analysis of the technology used
        - MVP Stage: Assessment of the product's development stage
        - Competitive Edge: How the solution compares to competitors
        - External Benchmarks: Industry comparisons and benchmarks
        - Conclusion: Overall assessment of the innovation potential
        
        4. Create a comprehensive Market Pulse assessment that includes:
        - Market Opportunity: Overview of the market opportunity
        - TAM/SAM/SOM: Analysis of total addressable, serviceable, and obtainable markets
        - Growth Trends: Key market growth trends and projections
        - Business Model & Traction: Assessment of the business model and current traction
        - Revenue Model: Analysis of how the startup generates revenue
        - Financial Metrics: Key financial metrics and projections
        - External Validation: Market validation from external sources
        - Conclusion: Overall assessment of market opportunity and business viability
        
        5. Create a comprehensive Thesis Fit Score assessment that includes:
        - Investor Criteria & Match Breakdown: Summary of investor requirements and alignment
        - Industry Alignment: How well the startup's industry matches investor focus
        - Geographical & Stage Fit: Match with investor's geographical preferences and stage criteria
        - Funding & Exit: Alignment with investor's funding amount and exit timeline expectations
        - Technology & Business Model: Match with investor's technology and business model preferences
        - Overall Fit Score: Percentage match estimation with brief explanation of alignment
        
        6. Create a Final Recommendation in approximately 200 words that:
        - Summarizes the key points from all assessments
        - Highlights the unique value proposition and opportunity
        - Addresses potential risks and mitigating factors
        - Provides a clear, actionable recommendation for the investor
        - Explains why this opportunity is worth considering

        OUTPUT FORMAT:
        Your response must be a valid JSON object with this EXACT structure.
        IMPORTANT: Keep all text fields SHORT and SIMPLE. Do not use complex formatting or special characters.
        
        {{
        "Executive Summary": "Startup Name",
        "Signal Strength": {{
            "Problem Overview": "Brief description of the problem statement. Strictly 200 words and proper analysis",
            "Validation & Supporting Data": "Brief summary of validation evidence. Strictly 200 words and proper analysis",
            "External Research": "Brief summary of external research. Strictly 200 words and proper analysis",
            "Data Matching": "Brief summary of data matching. Strictly 200 words and proper analysis",
            "Conclusion": "Brief overall assessment of signal strength and credibility of the startup. Strictly 200 words and proper analysis"
            }},
            "Innovation Index": {{
                "Solution Overview": "Brief description of the solution approach. Strictly 200 words and proper analysis",
                "Technology & Differentiation": "Brief analysis of technology used. Strictly 200 words and proper analysis",
                "MVP Stage": "Brief assessment of product development stage. Strictly 200 words and proper analysis",
                "Competitive Edge": "Brief analysis of competitive advantage. Strictly 200 words and proper analysis",
                "External Benchmarks": "Brief summary of industry comparisons. Strictly 200 words and proper analysis",
                "Conclusion": "Brief overall assessment of innovation. Strictly 200 words and proper analysis"
            }},
            "Market Pulse": {{
                "Market Opportunity": "Brief overview of the market opportunity. Strictly 200 words and proper analysis",
                "TAM/SAM/SOM": "Brief analysis of total addressable, serviceable, and obtainable markets. Strictly 200 words and proper analysis",
                "Growth Trends": "Brief summary of key market growth trends. Strictly 200 words and proper analysis",
                "Business Model & Traction": "Brief assessment of business model and current traction. Strictly 200 words and proper analysis",
                "Revenue Model": "Brief analysis of revenue generation approach. Strictly 200 words and proper analysis",
                "Financial Metrics": "Brief summary of key financial metrics. Strictly 200 words and proper analysis",
                "External Validation": "Brief summary of market validation from external sources. Strictly 200 words and proper analysis",
                "Conclusion": "Brief overall assessment of market opportunity and business viability. Strictly 200 words and proper analysis"
            }},
            "Thesis Fit Score": {{
                "Investor Criteria & Match Breakdown": "Brief summary of investor criteria alignment. Strictly 200 words and proper analysis",
                "Industry Alignment": "Brief assessment of industry match.Strictly 200 words and proper analysis",
                "Geographical & Stage Fit": "Brief analysis of location and stage match. Strictly 200 words and proper analysis",
                "Funding & Exit": "Brief assessment of funding and exit alignment. Strictly 200 words and proper analysis",
                "Technology & Business Model": "Brief analysis of tech and business model match. Strictly 200 words and proper analysis",
                "Overall Fit Score": "Percentage match with brief explanation. Strictly 200 words and proper analysis"
            }},
            "Final Recommendation": "A concise 200-word summary that explains the opportunity uniquely and effectively to the investor, including key points, unique value, risks, and actionable recommendation"
        }}

        CRITICAL JSON FORMATTING RULES:
        - Keep most string values SHORT (under 100 words each)
        - The Final Recommendation can be up to 200 words
        - DO NOT use line breaks within string values
        - DO NOT use quotation marks within string values
        - DO NOT use special characters that need escaping
        - Use plain text only in all string values
        - Keep punctuation simple (periods, commas only)
        - Return ONLY the JSON with no additional text
        """,
        agent=analyst,
        expected_output="A JSON object with executive summary, assessments, and final recommendation"
    )
    
    # Create the crew with just the analyst
    crew = Crew(
        agents=[analyst],
        tasks=[analysis_task],
        verbose=True,
        process=Process.sequential
    )
    
    # Run the crew
    try:
        crew_output = crew.kickoff()
        
        # Try to get the string result
        result_str = None
        
        # Method 1: Try to access tasks_output
        if hasattr(crew_output, 'tasks_output'):
            # Access first task output
            if hasattr(crew_output.tasks_output, '__getitem__'):
                task_result = crew_output.tasks_output[0]
                
                # Try different attributes for the string value
                if hasattr(task_result, 'output'):
                    result_str = task_result.output
                elif hasattr(task_result, 'value'):
                    result_str = task_result.value
                elif hasattr(task_result, '__str__'):
                    result_str = str(task_result)
        
        # Method 2: Try direct string representation as fallback
        if not result_str:
            result_str = str(crew_output)
        
        logger.info(f"Result string preview: {result_str[:200]}...")
        
        # Method 3: Try to extract the complete JSON object from the response
        json_match = re.search(r'(\{.*\})', result_str, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
            logger.info(f"Extracted potential JSON: {json_str[:100]}...")
            
            try:
                extracted_json = json.loads(json_str)
                logger.info("Successfully parsed extracted JSON")
                return extracted_json
            except json.JSONDecodeError:
                logger.warning("Failed to parse extracted JSON, continuing with field extraction")
        
        # Process the result - first try direct JSON parsing
        try:
            # Try to parse the entire response as JSON
            json_data = json.loads(result_str)
            logger.info("Successfully parsed result as complete JSON")
            
            # Use the parsed JSON directly
            summary_data = json_data
            
        except json.JSONDecodeError:
            logger.warning("Direct JSON parsing failed, using regex extraction as fallback")
            
            # Enhanced extraction functions
            def extract_simple_field(field_name, default="Unable to extract"):
                pattern = r'"' + re.escape(field_name) + r'"\s*:\s*"([^"]+)"'
                match = re.search(pattern, result_str)
                return match.group(1) if match else default
            
            def extract_section_field(section, field, default="Unable to extract"):
                try:
                    # First try: most precise pattern
                    pattern = r'"' + re.escape(section) + r'"\s*:\s*{[^{]*?"' + re.escape(field) + r'"\s*:\s*"([^"]+)"'
                    match = re.search(pattern, result_str, re.DOTALL)
                    if match:
                        return match.group(1)
                    
                    # Second try: more lenient pattern with any content between section and field
                    pattern = r'"' + re.escape(section) + r'".*?"' + re.escape(field) + r'"\s*:\s*"([^"]+)"'
                    match = re.search(pattern, result_str, re.DOTALL)
                    if match:
                        return match.group(1)
                    
                    # Third try: extract the entire section first, then find the field within it
                    section_pattern = r'"' + re.escape(section) + r'"\s*:\s*{([^}]+)}'
                    section_match = re.search(section_pattern, result_str, re.DOTALL)
                    if section_match:
                        section_content = section_match.group(1)
                        field_pattern = r'"' + re.escape(field) + r'"\s*:\s*"([^"]+)"'
                        field_match = re.search(field_pattern, section_content)
                        if field_match:
                            return field_match.group(1)
                    
                    # Fourth try: even more lenient pattern for deeply nested fields
                    super_lenient_pattern = r'"' + re.escape(field) + r'"\s*:\s*"([^"]+)"'
                    super_match = re.search(super_lenient_pattern, result_str)
                    if super_match:
                        return super_match.group(1)
                    
                    # If all attempts fail, log detailed information for debugging
                    logger.warning(f"Could not extract '{field}' from section '{section}'")
                    return default
                    
                except Exception as e:
                    logger.warning(f"Error extracting '{field}' from '{section}': {str(e)}")
                    return default
            
            # Extract startup name and final recommendation directly
            startup_name = extract_simple_field("Executive Summary", "Unknown Startup")
            final_recommendation = extract_simple_field("Final Recommendation", "Unable to extract final recommendation")
            
            # Try to parse the JSON structure using a more robust approach
            def extract_all_sections():
                all_sections = {}
                
                # Find all section blocks in the JSON
                section_pattern = r'"([^"]+)"\s*:\s*{([^}]+)}'
                sections = re.findall(section_pattern, result_str, re.DOTALL)
                
                for section_name, section_content in sections:
                    # Find all fields in each section
                    field_pattern = r'"([^"]+)"\s*:\s*"([^"]+)"'
                    fields = re.findall(field_pattern, section_content)
                    
                    section_data = {field[0]: field[1] for field in fields}
                    all_sections[section_name] = section_data
                
                return all_sections
            
            # Try the robust section extraction
            all_sections = extract_all_sections()
            
            # If we found some sections, use them
            if all_sections:
                logger.info(f"Found {len(all_sections)} sections in the JSON structure")
                
                # Ensure we have all the main sections
                required_sections = ["Signal Strength", "Innovation Index", "Market Pulse", "Thesis Fit Score"]
                has_all_sections = all([section in all_sections for section in required_sections])
                
                if has_all_sections:
                    logger.info("Found all required sections, using extracted data")
                    
                    summary_data = {
                        "Executive Summary": startup_name,
                        **all_sections
                    }
                    
                    # Add the final recommendation separately
                    summary_data["Final Recommendation"] = final_recommendation
                    
                else:
                    logger.warning("Missing some required sections, falling back to manual extraction")
                    # Continue with the standard extraction approach
            
            # Construct the JSON manually with section-specific extraction as a fallback
            if not all_sections or not has_all_sections:
                summary_data = {
                    "Executive Summary": startup_name,
                    "Signal Strength": {
                        "Problem Overview": extract_section_field("Signal Strength", "Problem Overview"),
                        "Validation & Supporting Data": extract_section_field("Signal Strength", "Validation & Supporting Data"),
                        "External Research": extract_section_field("Signal Strength", "External Research"),
                        "Data Matching": extract_section_field("Signal Strength", "Data Matching"),
                        "Conclusion": extract_section_field("Signal Strength", "Conclusion")
                    },
                    "Innovation Index": {
                        "Solution Overview": extract_section_field("Innovation Index", "Solution Overview"),
                        "Technology & Differentiation": extract_section_field("Innovation Index", "Technology & Differentiation"),
                        "MVP Stage": extract_section_field("Innovation Index", "MVP Stage"),
                        "Competitive Edge": extract_section_field("Innovation Index", "Competitive Edge"),
                        "External Benchmarks": extract_section_field("Innovation Index", "External Benchmarks"),
                        "Conclusion": extract_section_field("Innovation Index", "Conclusion")
                    },
                    "Market Pulse": {
                        "Market Opportunity": extract_section_field("Market Pulse", "Market Opportunity"),
                        "TAM/SAM/SOM": extract_section_field("Market Pulse", "TAM/SAM/SOM"),
                        "Growth Trends": extract_section_field("Market Pulse", "Growth Trends"),
                        "Business Model & Traction": extract_section_field("Market Pulse", "Business Model & Traction"),
                        "Revenue Model": extract_section_field("Market Pulse", "Revenue Model"),
                        "Financial Metrics": extract_section_field("Market Pulse", "Financial Metrics"),
                        "External Validation": extract_section_field("Market Pulse", "External Validation"),
                        "Conclusion": extract_section_field("Market Pulse", "Conclusion")
                    },
                    "Thesis Fit Score": {
                        "Investor Criteria & Match Breakdown": extract_section_field("Thesis Fit Score", "Investor Criteria & Match Breakdown"),
                        "Industry Alignment": extract_section_field("Thesis Fit Score", "Industry Alignment"),
                        "Geographical & Stage Fit": extract_section_field("Thesis Fit Score", "Geographical & Stage Fit"),
                        "Funding & Exit": extract_section_field("Thesis Fit Score", "Funding & Exit"),
                        "Technology & Business Model": extract_section_field("Thesis Fit Score", "Technology & Business Model"),
                        "Overall Fit Score": extract_section_field("Thesis Fit Score", "Overall Fit Score")
                    },
                    "Final Recommendation": final_recommendation
                }
        
        logger.info("Successfully constructed JSON from extracted components")
        
        return summary_data
        
    except Exception as e:
        logger.error(f"Error running the analysis crew or constructing JSON: {str(e)}")
        return {
            "Executive Summary": f"Error: {str(e)}",
            "Signal Strength": {
                "Problem Overview": "Error occurred during analysis",
                "Validation & Supporting Data": "Error occurred during analysis",
                "External Research": "Error occurred during analysis",
                "Data Matching": "Error occurred during analysis",
                "Conclusion": "Error occurred during analysis"
            },
            "Innovation Index": {
                "Solution Overview": "Error occurred during analysis",
                "Technology & Differentiation": "Error occurred during analysis",
                "MVP Stage": "Error occurred during analysis",
                "Competitive Edge": "Error occurred during analysis",
                "External Benchmarks": "Error occurred during analysis",
                "Conclusion": "Error occurred during analysis"
            },
            "Market Pulse": {
                "Market Opportunity": "Error occurred during analysis",
                "TAM/SAM/SOM": "Error occurred during analysis",
                "Growth Trends": "Error occurred during analysis",
                "Business Model & Traction": "Error occurred during analysis",
                "Revenue Model": "Error occurred during analysis",
                "Financial Metrics": "Error occurred during analysis",
                "External Validation": "Error occurred during analysis",
                "Conclusion": "Error occurred during analysis"
            },
            "Thesis Fit Score": {
                "Investor Criteria & Match Breakdown": "Error occurred during analysis",
                "Industry Alignment": "Error occurred during analysis",
                "Geographical & Stage Fit": "Error occurred during analysis",
                "Funding & Exit": "Error occurred during analysis",
                "Technology & Business Model": "Error occurred during analysis",
                "Overall Fit Score": "Error occurred during analysis"
            },
            "Final Recommendation": "Error occurred during analysis"
        }

def create_investment_summary_column():
    """
    Create InvestmentSummaryAgent column if it doesn't exist
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if column exists
        check_query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'pitch_decks' AND column_name = 'InvestmentSummaryAgent';
        """
        
        cursor.execute(check_query)
        column_exists = cursor.fetchone()
        
        # If column doesn't exist, add it
        if not column_exists:
            # Check if timestamp column exists
            check_timestamp_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'pitch_decks' AND column_name = 'investment_summary_last_updated';
            """
            
            cursor.execute(check_timestamp_query)
            timestamp_exists = cursor.fetchone()
            
            # Create columns based on what's missing
            if not timestamp_exists:
                alter_query = """
                ALTER TABLE pitch_decks
                ADD COLUMN "InvestmentSummaryAgent" JSONB,
                ADD COLUMN investment_summary_last_updated TIMESTAMP;
                """
            else:
                alter_query = """
                ALTER TABLE pitch_decks
                ADD COLUMN "InvestmentSummaryAgent" JSONB;
                """
            
            cursor.execute(alter_query)
            conn.commit()
            logger.info("Successfully added InvestmentSummaryAgent column")
        else:
            logger.info("InvestmentSummaryAgent column already exists")
    
    except Exception as e:
        logger.error(f"Error checking/adding column: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
    
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def save_investment_summary(pitch_id: int, investment_summary: Dict[str, Any]) -> bool:
    """
    Save investment summary to the pitch_decks table
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Convert investment summary to JSON string
        investment_summary_json = json.dumps(investment_summary)
        
        # Update query to save InvestmentSummaryAgent
        update_query = """
        UPDATE pitch_decks 
        SET "InvestmentSummaryAgent" = %s,
            investment_summary_last_updated = CURRENT_TIMESTAMP
        WHERE pitch_id = %s
        """
        
        # Execute the update
        cursor.execute(update_query, (investment_summary_json, pitch_id))
        
        # Commit the transaction
        conn.commit()
        
        logger.info(f"Successfully saved investment summary for pitch ID {pitch_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving investment summary for pitch ID {pitch_id}: {str(e)}")
        # Rollback in case of error
        if 'conn' in locals() and conn:
            conn.rollback()
        return False
        
    finally:
        # Ensure cursor and connection are closed
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    # Get pitch ID from command line or user input
    import sys
    
    if len(sys.argv) > 1:
        pitch_id = int(sys.argv[1])
    else:
        pitch_id = int(input("Enter the pitch deck ID to analyze: "))
    
    # Ensure the InvestmentSummaryAgent column exists
    create_investment_summary_column()
    
    # Generate the executive summary
    investment_summary = generate_executive_summary(pitch_id)
    
    # Print the result as formatted JSON
    print(json.dumps(investment_summary, indent=2))
    
    # Save the results to the database
    if "error" not in investment_summary:
        save_success = save_investment_summary(pitch_id, investment_summary)
        if save_success:
            print(f"Investment summary successfully saved to database for pitch ID {pitch_id}")
        else:
            print(f"Failed to save investment summary to database for pitch ID {pitch_id}")
    else:
        print(f"Not saving investment summary due to error: {investment_summary.get('error')}")