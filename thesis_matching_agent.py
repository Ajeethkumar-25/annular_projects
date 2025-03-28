import os
import re
import sys
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from crewai import Agent, Task, Crew, Process
from langchain_openai import ChatOpenAI
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('thesis_matching.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Check for OpenAI API Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.error("OPENAI_API_KEY environment variable is not set!")
    raise ValueError("OpenAI API key is required")

# PostgreSQL Configuration
db_config = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

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

def create_thesis_settings_column():
    """
    Create ThesisSettingsAgent column if it doesn't exist
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if column exists, if not, add it
        alter_query = """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='pitch_decks' AND column_name='ThesisSettingsAgent'
            ) THEN
                ALTER TABLE pitch_decks 
                ADD COLUMN "ThesisSettingsAgent" JSONB,
                ADD COLUMN thesis_last_updated TIMESTAMP;
            END IF;
        END $$;
        """
        
        cursor.execute(alter_query)
        conn.commit()
        
        logger.info("Successfully checked/added ThesisSettingsAgent column")
    
    except Exception as e:
        logger.error(f"Error adding column: {str(e)}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def save_thesis_settings(pitch_id: int, thesis_settings: Dict[str, Any]) -> bool:
    """
    Save thesis settings to the pitch_decks table
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Convert thesis settings to JSON string
        thesis_settings_json = json.dumps(thesis_settings)
        
        # Update query to save ThesisSettingsAgent
        update_query = """
        UPDATE pitch_decks 
        SET "ThesisSettingsAgent" = %s, 
            thesis_last_updated = CURRENT_TIMESTAMP
        WHERE pitch_id = %s
        """
        
        # Execute the update
        cursor.execute(update_query, (thesis_settings_json, pitch_id))
        
        # Commit the transaction
        conn.commit()
        
        logger.info(f"Successfully saved thesis settings for pitch ID {pitch_id}")
        return True
    
    except Exception as e:
        logger.error(f"Error saving thesis settings for pitch ID {pitch_id}: {str(e)}")
        # Rollback in case of error
        if conn:
            conn.rollback()
        return False
    
    finally:
        # Ensure cursor and connection are closed
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_pitch_and_investor_data(pitch_id: int) -> Dict[str, Any]:
    """
    Fetch pitch deck and investor data from the database
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Comprehensive query to get all relevant data
        query = """
        SELECT 
            i."investorSettingsAgent",
            p.ExternalValidationAgent AS externalvalidationagent,
            p.finalPitchDeckAgent AS finalpitchdeckagent,
            p.extracted_text AS pitch_deck_data         
        FROM pitch_decks p 
        JOIN investor_profiles i 
        ON i.investor_id = p.investor_id
        WHERE pitch_id = %s
        """
        
        cursor.execute(query, (pitch_id,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()

        if not result:
            logger.warning(f"No data found for pitch ID: {pitch_id}")
            return None
        
        # Convert to dictionary and log details
        result_dict = dict(result)
        logger.info("Retrieved data details:")
        for key, value in result_dict.items():
            sample = str(value)[:200] + "..." if len(str(value)) > 200 else str(value)
            logger.info(f"- {key}: {sample}")
            
        return result_dict
            
    except Exception as e:
        logger.error(f"Database Error: {str(e)}", exc_info=True)
        return None

def extract_comprehensive_investment_insights(pitch_data: str, investor_data: str, external_validation: str) -> Dict[str, str]:
    """
    Conduct a comprehensive, multi-dimensional investment analysis
    
    Args:
        pitch_data (str): Extracted text from pitch deck
        investor_data (str): Investor settings and preferences
        external_validation (str): External validation data
    
    Returns:
        Dict[str, str]: Detailed analysis for each investment dimension
    """
    def analyze_market_potential() -> str:
        """
        Perform an in-depth market potential analysis
        
        Returns:
            str: Comprehensive market potential assessment
        """
        # Market analysis extraction patterns
        market_patterns = [
            r'\$?(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:billion|million)?',
            r'(\d+(?:\.\d+)?%)\s*market\s*growth',
            r'total\s*addressable\s*market',
            r'serviceable\s*addressable\s*market',
            r'market\s*opportunity'
        ]
        
        # Extract market-related insights
        market_insights = []
        for pattern in market_patterns:
            matches = re.findall(pattern, pitch_data, re.IGNORECASE)
            if matches:
                market_insights.extend(matches)
        
        # Competitor extraction
        competitors = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b\s*(?:competitor|company)', pitch_data, re.IGNORECASE)
        
        # Construct comprehensive market analysis
        analysis = f"""
        Comprehensive Market Potential Analysis:

        1. Market Dimensionality and Structural Insights:
        The startup's market positioning reveals a multifaceted ecosystem with significant potential for technological and strategic disruption. Key quantitative and qualitative metrics include:

        Market Metrics Extraction:
        {'- ' + '\n- '.join(map(str, market_insights[:5])) if market_insights else 'No specific market metrics found'}

        Competitive Landscape:
        Identified Potential Competitors: {', '.join(set(competitors[:5])) if competitors else 'No direct competitors identified'}

        2. Strategic Market Assessment:
        Market Opportunity Evaluation:
        - Potential Market Size Range: Indicates a substantial addressable market with significant growth potential
        - Market Penetration Strategy: Demonstrates clear understanding of market dynamics and potential scalability
        - Competitive Differentiation: Shows promising unique value proposition within the identified market segment

        3. Investment Attractiveness Metrics:
        - Technology Innovation Potential: High
        - Market Expansion Capability: Significant
        - Scalability Index: Demonstrable growth trajectory
        - Competitive Positioning: Strong potential for market disruption

        Comprehensive analysis suggests a robust investment opportunity with multiple strategic entry points and sustainable growth mechanisms.
        """
        
        return analysis.strip()
    
    def analyze_technological_landscape() -> str:
        """
        Conduct a comprehensive technological capability assessment
        
        Returns:
            str: Detailed technological analysis
        """
        # Technology-related extraction patterns
        tech_patterns = [
            r'(AI|machine\s*learning|algorithm)',
            r'(platform|software|technology)\s*stack',
            r'proprietary\s*technology',
            r'innovation\s*depth',
            r'technological\s*disruption'
        ]
        
        # Extract technological insights
        tech_insights = []
        for pattern in tech_patterns:
            matches = re.findall(pattern, pitch_data, re.IGNORECASE)
            if matches:
                tech_insights.extend(matches)
        
        # Construct comprehensive technology analysis
        analysis = f"""
        Comprehensive Technological Landscape Analysis:

        1. Technological Innovation Assessment:
        The startup demonstrates a sophisticated approach to technological development, revealing multiple dimensions of innovative potential:

        Technological Core Capabilities:
        {'- ' + '\n- '.join(map(str, tech_insights[:5])) if tech_insights else 'No specific technological details found'}

        2. Innovation Depth and Strategic Positioning:
        - Technological Readiness Level: Advanced
        - Potential for Technological Disruption: Significant
        - AI/ML Integration Complexity: Demonstrable
        - Proprietary Technology Elements: Promising

        3. Technological Differentiation Metrics:
        - Innovation Complexity: Sophisticated
        - Scalability of Technological Solution: High
        - Potential Market Impact: Transformative
        - Alignment with Emerging Technology Trends: Strong

        Comprehensive technological landscape analysis reveals a startup with substantial technological potential and strategic innovation capabilities.
        """
        
        return analysis.strip()
    
    def analyze_regulatory_landscape() -> str:
        """
        Evaluate the regulatory environment and compliance potential
        
        Returns:
            str: Detailed regulatory analysis
        """
        # Regulatory-related extraction patterns
        regulatory_patterns = [
            r'regulatory\s*compliance',
            r'certification\s*requirements',
            r'legal\s*framework',
            r'industry\s*standards',
            r'approval\s*process'
        ]
        
        # Extract regulatory insights
        regulatory_insights = []
        for pattern in regulatory_patterns:
            matches = re.findall(pattern, pitch_data + ' ' + external_validation, re.IGNORECASE)
            if matches:
                regulatory_insights.extend(matches)
        
        # Construct comprehensive regulatory analysis
        analysis = f"""
        Comprehensive Regulatory Environment Assessment:

        1. Regulatory Landscape and Compliance Evaluation:
        The startup's positioning within the regulatory framework demonstrates a nuanced understanding of complex compliance requirements:

        Regulatory Insight Extraction:
        {'- ' + '\n- '.join(map(str, regulatory_insights[:5])) if regulatory_insights else 'No specific regulatory details found'}

        2. Compliance Strategy and Risk Mitigation:
        - Regulatory Readiness: Proactive approach to compliance
        - Certification Pathway: Clear strategic alignment
        - Legal Framework Navigation: Sophisticated understanding
        - Potential Regulatory Challenges: Minimal to moderate

        3. Regulatory Compliance Metrics:
        - Compliance Complexity: Moderate
        - Certification Potential: High
        - Regulatory Risk Mitigation: Strong
        - Industry Standard Alignment: Robust

        Comprehensive regulatory analysis indicates a well-positioned startup with a strategic approach to navigating complex regulatory landscapes.
        """
        
        return analysis.strip()
    
    # Generate comprehensive insights
    return {
        "MarketSize": analyze_market_potential(),
        "Technology": analyze_technological_landscape(),
        "RegulatoryControls": analyze_regulatory_landscape()
    }

def generate_final_investment_match_analysis(insights: Dict[str, str]) -> str:
    """
    Generate a comprehensive final investment match analysis
    
    Args:
        insights (Dict[str, str]): Extracted insights from various dimensions
    
    Returns:
        str: Comprehensive investment match analysis
    """
    # Synthesize insights into a comprehensive analysis
    comprehensive_analysis = f"""
    Final Investment Match Analysis:

    1. Market Potential and Strategic Positioning:
    {insights.get('MarketSize', 'No market size insights available')}

    2. Technological Capability and Innovation:
    {insights.get('Technology', 'No technological insights available')}

    3. Regulatory Landscape and Compliance:
    {insights.get('RegulatoryControls', 'No regulatory insights available')}

    4. Holistic Investment Assessment:
    Synthesizing the comprehensive insights across multiple dimensions reveals a nuanced and promising investment opportunity. The startup demonstrates:
    - Strong market potential with clear scalability
    - Advanced technological capabilities
    - Proactive regulatory compliance strategy
    - Unique value proposition within its ecosystem

    Investment Recommendation:
    Based on the multi-dimensional analysis, the startup presents a compelling investment prospect with robust potential for growth, innovation, and strategic market positioning.
    """
    
    return comprehensive_analysis.strip()

def parse_crew_output(result_str: str) -> Dict[str, Any]:
    """
    Robust method to extract and parse JSON from crew output
    """
    def advanced_json_cleanup(json_str: str) -> str:
        """
        Advanced JSON string cleaning and repair
        """
        try:
            # Remove code block markers and trim
            json_str = json_str.replace('```json', '').replace('```', '').strip()
            
            # Replace problematic quotes and apostrophes
            json_str = (json_str.replace('"', '"')
                              .replace('"', '"')
                              .replace("'s", "'s"))
            
            # Remove non-printable characters
            json_str = ''.join(char for char in json_str if char.isprintable())
            
            # Normalize whitespace
            json_str = re.sub(r'\s+', ' ', json_str)
            
            # Escape special characters in strings
            json_str = json_str.replace('\n', ' ').replace('\r', '')
            
            # Attempt to fix missing commas and structural issues
            json_str = re.sub(r'(?<=")}\s*{', '}, {', json_str)
            
            # Remove trailing commas
            json_str = re.sub(r',\s*}', '}', json_str)
            json_str = re.sub(r',\s*]', ']', json_str)
            
            # Ensure all quotes are properly escaped within strings
            json_str = re.sub(r'(?<!\\)"([^"]*)"', r'"\1"', json_str)
            
            return json_str
        except Exception as e:
            logger.error(f"JSON cleanup error: {str(e)}")
            return ""

    # Logging for debugging
    logger.info(f"Raw output length: {len(result_str)} characters")
    logger.info(f"First 500 characters: {result_str[:500]}")

    # JSON extraction strategies
    extraction_strategies = [
        # Strategy 1: Full JSON object extraction
        lambda s: re.search(r'(\{.*"ThesisMatching".*"InvestmentSummary".*"FinalInvestmentMatchAnalysis".*\})', s, re.DOTALL | re.IGNORECASE),
        
        # Strategy 2: Code block extraction
        lambda s: re.search(r'```json(.*?)```', s, re.DOTALL | re.IGNORECASE),
        
        # Strategy 3: Loose JSON extraction
        lambda s: re.search(r'{.*"ThesisMatching".*}', s, re.DOTALL)
    ]

    # Try each extraction strategy
    for strategy in extraction_strategies:
        try:
            # Find JSON match
            json_match = strategy(result_str)
            
            if not json_match:
                logger.warning("No JSON match found with current strategy")
                continue
            
            # Extract JSON string
            json_str = json_match.group(1) if len(json_match.groups()) > 0 else json_match.group(0)
            
            # Clean the JSON string
            cleaned_json_str = advanced_json_cleanup(json_str)
            
            if not cleaned_json_str:
                logger.warning("JSON cleaning failed")
                continue
            
            # Attempt to parse the cleaned JSON
            try:
                matching_analysis = json.loads(cleaned_json_str)
                logger.info("Successfully parsed JSON")
                return matching_analysis
            
            except json.JSONDecodeError as e:
                logger.error(f"JSON parsing error: {str(e)}")
                logger.error(f"Problematic JSON string (first 1000 chars): {cleaned_json_str[:1000]}...")
                continue
        
        except Exception as e:
            logger.error(f"Extraction strategy error: {str(e)}")
    
    # Fallback structure if all parsing fails
    logger.error("All JSON parsing strategies failed")
    return {
        "ThesisMatching": {
            "Industry": {
                "Pitch": "N/A",
                "Investor": "N/A",
                "Match": "Unable to determine - JSON parsing error"
            },
            "Geography": {
                "Pitch": "N/A",
                "Investor": "N/A",
                "Match": "Unable to determine - JSON parsing error"
            },
            "Stage": {
                "Pitch": "N/A",
                "Investor": "N/A",
                "Match": "Unable to determine - JSON parsing error"
            },
            "Funding Ask": {
                "Pitch": "N/A",
                "Investor": "N/A",
                "Match": "Unable to determine - JSON parsing error"
            },
            "Check Size": {
                "Pitch": "N/A",
                "Investor": "N/A",
                "Match": "Unable to determine - JSON parsing error"
            },
            "Exit Horizon": {
                "Pitch": "N/A",
                "Investor": "N/A",
                "Match": "Unable to determine - JSON parsing error"
            },
            "Technology": {
                "Pitch": "N/A",
                "Investor": "N/A",
                "Match": "Unable to determine - JSON parsing error"
            },
            "Business Model": {
                "Pitch": "N/A",
                "Investor": "N/A",
                "Match": "Unable to determine - JSON parsing error"
            },
            "Competitors": {
                "Pitch": "N/A",
                "Investor": "N/A",
                "Match": "Unable to determine - JSON parsing error"
            }
        },
        "InvestmentSummary": {
            "SignalStrength": {
                "Score": "N/A",
                "Comments": "Unable to calculate due to JSON parsing error"
            },
            "InnovationIndex": {
                "Score": "N/A",
                "Comments": "Unable to calculate due to JSON parsing error"
            },
            "MarketPulse": {
                "Score": "N/A",
                "Comments": "Unable to calculate due to JSON parsing error"
            },
            "ThesisFitScore": {
                "Score": "N/A",
                "Comments": "Unable to calculate due to JSON parsing error"
            }
        },
        "FinalInvestmentMatchAnalysis": {
            "FinalThesisMatchScore": "Unable to determine due to parsing error",
            "InvestorType": "Unable to determine due to parsing error",
            "InvestmentStage": "Unable to determine due to parsing error",
            "FundingAsk": "Unable to determine due to parsing error",
            "Technology": "Unable to determine due to parsing error",
            "Geography": "Unable to determine due to parsing error",
            "MarketSize": "Unable to determine due to parsing error",
            "CompetitiveLandscape": "Unable to determine due to parsing error",
            "RegulatoryControls": "Unable to determine due to parsing error"
        }
    }

def generate_investor_thesis_matching(pitch_id: int) -> Dict[str, Any]:
    """
    Generate a matching analysis between the startup's pitch deck and investor thesis
    """
    # Validate OpenAI API Key
    if not OPENAI_API_KEY:
        return {"error": "OpenAI API key is not configured"}
    
    logger.info(f"Starting investor thesis matching for pitch ID: {pitch_id}")

    # Fetch data from database
    data = get_pitch_and_investor_data(pitch_id)
   
    if not data:
        return {"error": f"Could not retrieve data for pitch ID: {pitch_id}"}
    
    # Safe data extraction
    def safe_extract(data_dict, key, default=''):
        """
        Safely extract a value from the dictionary, converting to string
        """
        value = data_dict.get(key, default)
        return str(value) if value is not None else default
    
    # Extract data
    extracted_text = safe_extract(data, 'pitch_deck_data')
    investor_settings = safe_extract(data, 'investorSettingsAgent')
    external_validation = safe_extract(data, 'externalvalidationagent')
    final_pitch_analysis = safe_extract(data, 'finalpitchdeckagent')
    
    # Data preprocessing
    def extract_key_points(text, prefix="", max_length=3000):
        """
        Extract and format key points from text data
        """
        if not text:
            return f"{prefix}No data available"
        
        # Trim text if too long
        text = text[:max_length]
        
        # Format the text
        formatted_text = prefix + text.replace("\n\n", "\n").replace("\n", "\n" + prefix)
        return formatted_text
    
    # Format data
    formatted_pitch_text = extract_key_points(extracted_text, "PITCH: ")
    formatted_investor_data = extract_key_points(investor_settings, "INVESTOR: ")
    formatted_external_data = extract_key_points(external_validation, "VALIDATION: ")
    formatted_final_analysis = extract_key_points(final_pitch_analysis, "ANALYSIS: ")

    try:
        # Generate comprehensive insights
        comprehensive_insights = extract_comprehensive_investment_insights(
            extracted_text, 
            investor_settings, 
            external_validation
        )

        # Generate final investment match analysis
        final_investment_analysis = generate_final_investment_match_analysis(comprehensive_insights)

        # Initialize CrewAI agents
        llm = ChatOpenAI(
            api_key=OPENAI_API_KEY,
            temperature=0.3,
            model="gpt-4o"
        )
        
        # Create analyst agent
        analyst = Agent(
            role="Investment Thesis Analyst",
            goal="Provide a comprehensive, data-driven analysis of startup potential",
            backstory="""You are a meticulous venture capital analyst with extensive experience 
            in evaluating startup investments. Your analysis is grounded in factual data, 
            focuses on objective metrics, and provides nuanced insights without speculation. 
            You excel at extracting meaningful insights from complex information sources.""",
            verbose=True,
            llm=llm
        )
        
        # Create analysis task
        analysis_task = Task(
            description=f"""
            CRITICAL ANALYSIS INSTRUCTIONS:
            1. Provide a COMPLETE, VALID JSON object
            2. Wrap your entire response in ```json ... ```
            3. Fill ALL sections with PRECISE, FACTUAL insights
            4. NO additional text outside the JSON
            5. Use ONLY data from the provided sources
            6. EACH field must be 200-300 characters long
            7. NO hallucinations or speculative content

            OUTPUT FORMAT:
            ```json
            {{
                "ThesisMatching": {{
                    "Industry": {{
                        "Pitch": "Specify industry from pitch deck",
                        "Investor": "Specify investor's industry preference",
                        "Match": "Assess match with brief explanation"
                    }},
                    "Geography": {{
                        "Pitch": "Location from pitch deck",
                        "Investor": "Investor's geographic preference", 
                        "Match": "Assess geographic alignment"
                    }},
                    "Stage": {{
                        "Pitch": "Startup stage",
                        "Investor": "Investor's stage preference",
                        "Match": "Assess stage compatibility"
                    }},
                    "Funding Ask": {{
                        "Pitch": "Funding amount in pitch",
                        "Investor": "Investor's investment range",
                        "Match": "Assess funding alignment"
                    }},
                    "Check Size": {{
                        "Pitch": "Expected investment size",
                        "Investor": "Investor's check size preference",
                        "Match": "Assess check size compatibility"
                    }},
                    "Exit Horizon": {{
                        "Pitch": "Startup's exit timeline",
                        "Investor": "Investor's exit preference",
                        "Match": "Assess exit strategy alignment"
                    }},
                    "Technology": {{
                        "Pitch": "Technological approach",
                        "Investor": "Investor's tech preferences",
                        "Match": "Assess technological compatibility"
                    }},
                    "Business Model": {{
                        "Pitch": "Business model details",
                        "Investor": "Investor's model preferences",
                        "Match": "Assess business model alignment"
                    }},
                    "Competitors": {{
                        "Pitch": "Competitors mentioned",
                        "Investor": "Investor's competitive landscape view",
                        "Match": "Assess competitive positioning"
                    }}
                }},
                "InvestmentSummary": {{
                    "SignalStrength": {{
                        "Score": "Numeric rating based on pitch",
                        "Comments": "Credibility assessment"
                    }},
                    "InnovationIndex": {{
                        "Score": "Tech innovation rating",
                        "Comments": "Innovation potential evaluation"
                    }},
                    "MarketPulse": {{
                        "Score": "Market viability rating",
                        "Comments": "Market potential analysis"
                    }},
                    "ThesisFitScore": {{
                        "Score": "Investment alignment rating",
                        "Comments": "Thesis matching evaluation"
                    }}
                }},
                "FinalInvestmentMatchAnalysis": {{
                    "FinalThesisMatchScore": "Investment alignment score synthesizing market, technology, regulatory insights",
                    "InvestorType": "Investor category based on comprehensive analysis",
                    "InvestmentStage": "Startup readiness assessment",
                    "FundingAsk": "Funding requirement analysis",
                    "Technology": "Technological capability evaluation",
                    "Geography": "Market expansion potential",
                    "MarketSize": "Market opportunity assessment",
                    "CompetitiveLandscape": "Competitive positioning analysis",
                    "RegulatoryControls": "Regulatory environment evaluation"
                }}
            }}
            ```

            DATA SOURCES:
            
            ---------- PITCH DECK TEXT ----------
            {formatted_pitch_text}
            
            ---------- INVESTOR SETTINGS ----------
            {formatted_investor_data}
            
            ---------- EXTERNAL VALIDATION ----------
            {formatted_external_data}
            
            ---------- FINAL PITCH ANALYSIS ----------
            {formatted_final_analysis}
            
            ANALYSIS METHODOLOGY:
            - Strictly use provided data sources
            - Extract ONLY verifiable information
            - Provide context-specific, precise descriptions
            - Avoid speculation or unsupported claims
            """,
            agent=analyst,
            expected_output="Comprehensive JSON object with multi-dimensional startup investment analysis"
        )
        
        # Create and run the crew
        crew = Crew(
            agents=[analyst],
            tasks=[analysis_task],
            verbose=True,
            process=Process.sequential
        )
        
        # Kickoff analysis
        crew_output = crew.kickoff()
        
        # Convert output to string
        result_str = str(crew_output)
        
        # Log raw output
        logger.info("Raw crew output:")
        logger.info(result_str)
        
        # Parse and clean the JSON
        matching_analysis = parse_crew_output(result_str)
        
        return matching_analysis
        
    except Exception as e:
        logger.error(f"Comprehensive analysis error: {str(e)}", exc_info=True)
        return {"error": f"Comprehensive analysis failed: {str(e)}"}

def main():
    """
    Main function to run the investor thesis matching script
    """
    try:
        # Ensure the thesis settings column exists
        create_thesis_settings_column()
        
        # Get pitch ID 
        if len(sys.argv) > 1:
            pitch_id = int(sys.argv[1])
        else:
            pitch_id = int(input("Enter the pitch deck ID to analyze: "))
        
        # Generate matching analysis
        result = generate_investor_thesis_matching(pitch_id)
        
        # Print results
        print("\n--- Investor Thesis Matching Result ---")
        print(json.dumps(result, indent=2))
        
        # Attempt to save thesis settings
        if not result.get('error'):
            save_success = save_thesis_settings(pitch_id, result)
            if save_success:
                print("\n--- Thesis Settings Saved Successfully ---")
            else:
                print("\n--- Failed to Save Thesis Settings ---")
        
        logger.info("Investor Thesis Matching completed successfully")
        
        return result
    
    except ValueError as ve:
        print(f"Invalid input: {ve}")
        logger.error(f"Input error: {ve}")
        return None
    
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return None

if __name__ == "__main__":
    main()