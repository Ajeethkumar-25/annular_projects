import os
import psycopg2
from psycopg2.extras import RealDictCursor
from crewai import Agent, Task, Crew, Process
from langchain.tools import Tool
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
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
      

def get_extracted_text_from_db(pitch_id):
    """
    Fetch extracted data from PostgreSQL database using pitch_id
    
    Args:
        pitch_id (int, str, dict): The ID of the pitch deck to retrieve
    
    Returns:
        dict or None: Extracted text or None if not found
    """
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Robust handling of pitch_id
        if isinstance(pitch_id, dict):
            # Extract the actual ID, handling different possible dict structures
            pitch_id = pitch_id.get('pitch_id') or \
                       pitch_id.get('id') or \
                       pitch_id.get('identifier')
        
        # Ensure pitch_id is a valid type (int or str)
        if not isinstance(pitch_id, (int, str)):
            logger.error(f"Invalid pitch_id type: {type(pitch_id)}")
            return None
        
        query = """
        SELECT 
            extracted_text
        FROM pitch_decks
        WHERE pitch_id = %s
        """
        
        cursor.execute(query, (pitch_id,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()

        if result:
            return result['extracted_text']  # Return only the extracted text
        else:
            logger.warning(f"No investor data found for ID: {pitch_id}")
            return None
            
    except psycopg2.Error as e:
        logger.error(f"❌ PostgreSQL Error: {str(e)}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected Error: {str(e)}", exc_info=True)
        return None

def get_extracted_text(text_content: str) -> Dict[str, Any]:
    """
    Extract key details from the provided pitch deck text content and validate against external sources.
    Uses prompt-based approaches for validation to get industry-specific insights.
    
    Args:
        text_content: The extracted text from a pitch deck
        
    Returns:
        Dictionary with structured data fields and external validation
    """
    # Check for OpenAI API Key before proceeding
    if not OPENAI_API_KEY:
        return {"error": "OpenAI API key is not configured. Please set the OPENAI_API_KEY environment variable."}
    
    logger.info("🔥 Extracting structured data from pitch deck text")
    
    # Initialize with default values for pitch deck data
    pitch_deck_data = {
        "Startup Name": "N/A",
        "Industry": "N/A",
        "Startup Stage": "N/A",
        "Funding Goal": "N/A",
        "Business Model": "N/A",
        "Core Technology": "N/A",
        "Revenue Model": "N/A",
        "Burn Rate": "N/A",
        "Projected 12M Revenue": "N/A",
        "Customer Base": "N/A",
        "Churn Rate": "N/A",
        "TAM": "N/A",
        "SAM": "N/A",
        "SOM": "N/A"
    }
    
    # Initialize external validation data
    external_validation = {
        "Industry Failure Patterns": {"summary": "N/A", "sources": []},
        "Market Size Validation": {"summary": "N/A", "sources": []},
        "Revenue Model Comparison": {"summary": "N/A", "sources": []},
        "Competitive Landscape": {"summary": "N/A", "sources": []},
        "Regulatory Barriers": {"summary": "N/A", "sources": []}
    }

    # Initialize tech & product maturity assessment data
    tech_product_maturity = {
        "AI Adoption in Industry": {"summary": "N/A", "sources": []},
        "Tech Stack Comparison": {"summary": "N/A", "sources": []},
        "Product Readiness": {"summary": "N/A", "sources": []}
    }
    
    try:
        # Create LLM client to use throughout the function
        llm = ChatOpenAI(
            api_key=OPENAI_API_KEY,
            model="gpt-4o",
            temperature=0.2
        )
        
        # Part 1: Extract pitch deck data
        logger.info("🔍 Extracting pitch deck data")
        
        extraction_prompt = f"""
        You are an expert venture capitalist who analyzes pitch decks for investments. I need you to extract specific information from the text of a pitch deck. The pitch deck may not explicitly label all fields, so use your expertise to identify and extract the information that best matches each category.

        INSTRUCTIONS:
        1. Analyze the text carefully to find the requested information.
        2. If the exact information is not labeled, look for related concepts and make reasonable inferences.
        3. If information for a field is truly not present, use "N/A".
        4. Return your answers ONLY as a JSON object with the exact field names provided.

        FIELDS TO EXTRACT:

        1. "Startup Name": The company name, typically on the first slide, in headers/footers, or after phrases like "introducing" or "about us".

        2. "Industry": The business sector or vertical (e.g., FinTech, Healthcare, SaaS, E-commerce). Look for explicit mentions or infer from the product/service description.

        3. "Startup Stage": Development phase (e.g., Pre-seed, Seed, Series A, Growth). May be mentioned in funding history or company timeline.

        4. "Funding Goal": The amount of money the startup is seeking to raise in this round. Look for phrases like "raising", "seeking", "investment opportunity" followed by dollar amounts.

        5. "Business Model": How the company generates value and operates (e.g., B2B, B2C, marketplace, subscription). Look for descriptions of how the business works.

        6. "Core Technology": The main technical innovation or platform the startup uses (e.g., AI, blockchain, proprietary algorithm). Look in sections about technology, product, or competitive advantage.

        7. "Revenue Model": How the company makes money (e.g., subscription, commission, freemium, pay-per-use). Look for mentions of pricing, revenue streams, or monetization.

        8. "Burn Rate": Monthly cash spend or rate at which company uses capital. Often in financial projections or funding slides.

        9. "Projected 12M Revenue": Expected revenue for the next 12 months. Look in financial projections, often labeled as "projected revenue" or shown in charts.

        10. "Customer Base": Description of current customers, either as specific numbers or general description. Look for "customers", "users", "clients", "partnerships".

        11. "Churn Rate": Customer attrition rate, often expressed as a percentage. May be in metrics, KPIs, or financial sections.

        12. "TAM" (Total Addressable Market): The total potential market size, usually the largest market figure mentioned, often in billions. Look for phrases like "total market size", "global opportunity".

        13. "SAM" (Serviceable Available Market): The portion of TAM the company can realistically target based on their business model and geography, typically smaller than TAM. Look for "target market" or "addressable market".

        14. "SOM" (Serviceable Obtainable Market): The realistic portion of SAM the company can capture in the near term (often 3-5 years). The smallest market figure, sometimes shown as percentage of SAM or in revenue projections.

        Text to analyze:
        {text_content}

        Return ONLY a JSON object with exactly these fields and nothing else. Ensure the JSON is properly formatted and can be parsed by a JSON parser. Do not include any markdown formatting, explanation, or additional text.
        """
        
        extraction_response = llm.invoke(extraction_prompt)
        extraction_content = extraction_response.content if hasattr(extraction_response, 'content') else str(extraction_response)
        
        # Extract and parse pitch deck data JSON
        try:
            # Try direct parsing first
            try:
                pitch_deck_parsed = json.loads(extraction_content)
                
                # Update pitch_deck_data with parsed values
                for key in pitch_deck_data:
                    if key in pitch_deck_parsed and pitch_deck_parsed[key] != "":
                        pitch_deck_data[key] = pitch_deck_parsed[key]
            except json.JSONDecodeError:
                # Fall back to regex extraction
                import re
                json_match = re.search(r'\{[\s\S]*?\}', extraction_content, re.DOTALL)
                
                if json_match:
                    json_str = json_match.group(0).replace('```json', '').replace('```', '')
                    pitch_deck_parsed = json.loads(json_str)
                    
                    # Update pitch_deck_data with parsed values
                    for key in pitch_deck_data:
                        if key in pitch_deck_parsed and pitch_deck_parsed[key] != "N/A" and pitch_deck_parsed[key] != "":
                            pitch_deck_data[key] = pitch_deck_parsed[key]
        except Exception as e:
            logger.error(f"Error extracting pitch deck data: {str(e)}")
        
        # Part 2: Generate external validation using a prompt-based approach
        logger.info("🔍 Generating external validation data")
        
        # Extract key values from pitch deck data for context
        industry = pitch_deck_data["Industry"] if pitch_deck_data["Industry"] != "N/A" else "technology"
        startup_name = pitch_deck_data["Startup Name"] if pitch_deck_data["Startup Name"] != "N/A" else "this startup"
        business_model = pitch_deck_data["Business Model"] if pitch_deck_data["Business Model"] != "N/A" else "B2B/B2C"
        core_tech = pitch_deck_data["Core Technology"] if pitch_deck_data["Core Technology"] != "N/A" else "emerging technology"
        revenue_model = pitch_deck_data["Revenue Model"] if pitch_deck_data["Revenue Model"] != "N/A" else "subscription"
        tam = pitch_deck_data["TAM"] if pitch_deck_data["TAM"] != "N/A" else "undisclosed"
        sam = pitch_deck_data["SAM"] if pitch_deck_data["SAM"] != "N/A" else "undisclosed"
        som = pitch_deck_data["SOM"] if pitch_deck_data["SOM"] != "N/A" else "undisclosed"
        
        # Generate external validation for each area
        for validation_area in external_validation.keys():
            try:
                validation_prompt = f"""
                You are an expert analyst specializing in the {industry} industry with deep knowledge of {validation_area}.
                
                You're examining {startup_name}, which has these characteristics:
                - Industry: {industry}
                - Business Model: {business_model}
                - Core Technology: {core_tech}
                - Revenue Model: {revenue_model}
                - Market Size (TAM/SAM/SOM): {tam}/{sam}/{som}
                
                Please provide a detailed analysis of "{validation_area}" specifically for this {industry} startup.
                Your analysis should:
                1. Include around 200 words with specific industry data, statistics, and benchmarks
                2. Mention relevant metrics, trends, or patterns specific to the {industry} sector
                3. Provide 3 specific, credible source URLs (not just names) that would be valuable for validating your analysis
                
                When providing sources, include ACTUAL URLs to research firms, industry reports, or publications that specialize 
                in the {industry} industry. These must be properly formatted URLs starting with http:// or https://.
                
                Format your response EXACTLY as a valid JSON object with these fields:
                {{
                  "summary": "your ~200 word analysis here with industry-specific insights",
                  "sources": [
                    "https://example1.com/industry-report",
                    "https://example2.com/research",
                    "https://example3.com/analysis"
                  ]
                }}
                
                Ensure your response is valid JSON that can be parsed directly. Do not include explanations or additional text.
                The sources MUST be valid URLs, not text descriptions.
                """
                
                validation_response = llm.invoke(validation_prompt)
                validation_content = validation_response.content if hasattr(validation_response, 'content') else str(validation_response)
                
                # Clean and parse JSON
                cleaned_content = clean_json_content(validation_content)
                validation_data = json.loads(cleaned_content)
                
                if "summary" in validation_data and validation_data["summary"]:
                    external_validation[validation_area]["summary"] = validation_data["summary"]
                if "sources" in validation_data and validation_data["sources"]:
                    external_validation[validation_area]["sources"] = validation_data["sources"]
                
            except Exception as e:
                logger.warning(f"Error generating {validation_area} validation: {str(e)}")
                # If the area generation fails, we'll rely on the fallback mechanism later
        
        # Part 3: Generate Tech & Product Maturity Assessment with prompt-based approach
        logger.info("🔍 Generating tech & product maturity assessment")
        
        # Generate tech maturity assessments for each area
        for tech_area in tech_product_maturity.keys():
            try:
                tech_prompt = f"""
                You are a technology analyst specializing in the {industry} industry with expertise in {tech_area}.
                
                You're assessing {startup_name}, which has these characteristics:
                - Industry: {industry}
                - Business Model: {business_model}
                - Core Technology: {core_tech}
                - Revenue Model: {revenue_model}
                - Startup Stage: {pitch_deck_data["Startup Stage"] if pitch_deck_data["Startup Stage"] != "N/A" else "early-stage"}
                - Customer Base: {pitch_deck_data["Customer Base"] if pitch_deck_data["Customer Base"] != "N/A" else "not disclosed"}
                
                Please provide a detailed assessment of "{tech_area}" specifically for this {industry} startup.
                Your assessment should:
                1. Include around 200 words with specific technology trends, benchmarks, and adoption patterns
                2. Provide concrete metrics, percentages, and industry-specific technology insights
                3. Include 3 specific, credible source URLs (not just names) for technology information in the {industry} sector
                
                When providing sources, include ACTUAL URLs to research firms, technology publications, or analyst groups
                that specialize in technology within the {industry} industry. These must be properly formatted URLs
                starting with http:// or https://.
                
                Format your response EXACTLY as a valid JSON object with these fields:
                {{
                  "summary": "your ~200 word analysis here with industry-specific technology insights",
                  "sources": [
                    "https://example1.com/tech-report",
                    "https://example2.com/research",
                    "https://example3.com/analysis"
                  ]
                }}
                
                Ensure your response is valid JSON that can be parsed directly. Do not include explanations or additional text.
                The sources MUST be valid URLs, not text descriptions.
                """
                
                tech_response = llm.invoke(tech_prompt)
                tech_content = tech_response.content if hasattr(tech_response, 'content') else str(tech_response)
                
                # Clean and parse JSON
                cleaned_content = clean_json_content(tech_content)
                tech_data = json.loads(cleaned_content)
                
                if "summary" in tech_data and tech_data["summary"]:
                    tech_product_maturity[tech_area]["summary"] = tech_data["summary"]
                if "sources" in tech_data and tech_data["sources"]:
                    tech_product_maturity[tech_area]["sources"] = tech_data["sources"]
                
            except Exception as e:
                logger.warning(f"Error generating {tech_area} assessment: {str(e)}")
                # If the area generation fails, we'll rely on the fallback mechanism later
        
        # Check if any validation areas are still missing and provide fallbacks
        for key in external_validation:
            if external_validation[key]["summary"] == "N/A" or not external_validation[key]["summary"]:
                # Generate a fallback with another prompt
                try:
                    fallback_prompt = f"""
                    You are a venture capital analyst providing a brief assessment of {key} for a {industry} startup called {startup_name}.
                    
                    Please provide:
                    1. A 150-word analysis of {key} specifically for the {industry} industry
                    2. Two source URLs where this information could be validated (must be properly formatted URLs starting with http:// or https://)
                    
                    Format your response ONLY as a valid JSON object:
                    {{
                      "summary": "your analysis here",
                      "sources": ["https://example1.com/report", "https://example2.com/analysis"]
                    }}
                    
                    The sources MUST be valid URLs, not text descriptions.
                    """
                    
                    fallback_response = llm.invoke(fallback_prompt)
                    fallback_content = fallback_response.content if hasattr(fallback_response, 'content') else str(fallback_response)
                    
                    # Clean and parse JSON
                    cleaned_content = clean_json_content(fallback_content)
                    fallback_data = json.loads(cleaned_content)
                    
                    if "summary" in fallback_data and fallback_data["summary"]:
                        external_validation[key]["summary"] = fallback_data["summary"]
                    if "sources" in fallback_data and fallback_data["sources"]:
                        external_validation[key]["sources"] = fallback_data["sources"]
                except Exception:
                    # Last resort fallback
                    external_validation[key]["summary"] = f"Analysis of {key} for {startup_name} in the {industry} industry would examine industry-specific metrics, competitive positioning, and market dynamics."
                    external_validation[key]["sources"] = [
                        f"{industry} Industry Association Annual Report",
                        f"Market Analysis for {industry} Sector by Leading Research Firm"
                    ]
        
        # Check if any tech assessment areas are still missing and provide fallbacks
        for key in tech_product_maturity:
            if tech_product_maturity[key]["summary"] == "N/A" or not tech_product_maturity[key]["summary"]:
                # Generate a fallback with another prompt
                try:
                    fallback_prompt = f"""
                    You are a technology analyst providing a brief assessment of {key} for a {industry} startup called {startup_name} using {core_tech} technology.
                    
                    Please provide:
                    1. A 150-word analysis of {key} specifically for the {industry} industry
                    2. Two source URLs where this information could be validated (must be properly formatted URLs starting with http:// or https://)
                    
                    Format your response ONLY as a valid JSON object:
                    {{
                      "summary": "your analysis here",
                      "sources": ["https://example1.com/tech-report", "https://example2.com/tech-analysis"]
                    }}
                    
                    The sources MUST be valid URLs, not text descriptions.
                    """
                    
                    fallback_response = llm.invoke(fallback_prompt)
                    fallback_content = fallback_response.content if hasattr(fallback_response, 'content') else str(fallback_response)
                    
                    # Clean and parse JSON
                    cleaned_content = clean_json_content(fallback_content)
                    fallback_data = json.loads(cleaned_content)
                    
                    if "summary" in fallback_data and fallback_data["summary"]:
                        tech_product_maturity[key]["summary"] = fallback_data["summary"]
                    if "sources" in fallback_data and fallback_data["sources"]:
                        tech_product_maturity[key]["sources"] = fallback_data["sources"]
                except Exception:
                    # Last resort fallback
                    tech_product_maturity[key]["summary"] = f"Assessment of {key} for {startup_name} in the {industry} industry would examine technology adoption patterns, implementation challenges, and integration opportunities."
                    tech_product_maturity[key]["sources"] = [
                        f"Technology Trends in {industry} Report",
                        f"Digital Transformation Benchmarks for {industry} Sector"
                    ]
    
    except Exception as e:
        logger.error(f"Error in pitch deck analysis: {str(e)}")
        # Generate fallbacks for all areas
        generate_fallbacks(external_validation, tech_product_maturity, pitch_deck_data)
    
    # Combine results into final structure
    return {
        "pitch_deck_data": pitch_deck_data,
        "external_validation": external_validation,
        "tech_product_maturity": tech_product_maturity
    }


def clean_json_content(content):
    """Clean LLM response to extract valid JSON"""
    import re
    
    # Remove markdown code blocks
    content = re.sub(r'```json\s*', '', content)
    content = re.sub(r'```\s*', '', content)
    
    # Extract JSON object
    match = re.search(r'\{[\s\S]*\}', content)
    if match:
        json_str = match.group(0)
        
        # Fix common JSON formatting issues
        json_str = re.sub(r'([{,])\s*([a-zA-Z0-9_]+)\s*:', r'\1"\2":', json_str)  # Fix missing quotes around keys
        json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
        json_str = re.sub(r',\s*]', ']', json_str)  # Remove trailing commas in arrays
        
        return json_str
    
    return "{}"  # Return empty JSON object if no match found


def generate_fallbacks(external_validation, tech_product_maturity, pitch_deck_data):
    """Generate fallback content for all validation areas"""
    industry = pitch_deck_data["Industry"] if pitch_deck_data["Industry"] != "N/A" else "technology"
    startup_name = pitch_deck_data["Startup Name"] if pitch_deck_data["Startup Name"] != "N/A" else "this startup"
    
    # Generate fallbacks for external validation
    for key in external_validation:
        if external_validation[key]["summary"] == "N/A" or not external_validation[key]["summary"]:
            external_validation[key]["summary"] = f"Analysis of {key} for {startup_name} in the {industry} industry would examine industry-specific metrics, competitive positioning, and market dynamics."
            external_validation[key]["sources"] = [
                f"https://industry-research.com/{industry.lower().replace(' ', '-')}-analysis",
                f"https://market-intelligence.com/{industry.lower().replace(' ', '-')}-report",
                f"https://competitive-analysis.org/{industry.lower().replace(' ', '-')}-landscape"
            ]
    
    # Generate fallbacks for tech product maturity
    for key in tech_product_maturity:
        if tech_product_maturity[key]["summary"] == "N/A" or not tech_product_maturity[key]["summary"]:
            tech_product_maturity[key]["summary"] = f"Assessment of {key} for {startup_name} in the {industry} industry would examine technology adoption patterns, implementation challenges, and integration opportunities."
            tech_product_maturity[key]["sources"] = [
                f"https://tech-research.com/{industry.lower().replace(' ', '-')}-technology-trends",
                f"https://digital-transformation.com/{industry.lower().replace(' ', '-')}-benchmarks",
                f"https://tech-adoption.org/{industry.lower().replace(' ', '-')}-implementation"
            ]
    # Combine results into final structure
    return {
        "pitch_deck_data": pitch_deck_data,
        "external_validation": external_validation,
        "tech_product_maturity": tech_product_maturity
    }

def generate_final_output(analysis_data, llm):
    """
    Generate a final assessment based on the complete pitch deck analysis.
    
    Args:
        analysis_data: Dictionary containing pitch_deck_data, external_validation, and tech_product_maturity
        llm: Initialized LLM client
        
    Returns:
        Dictionary with signal strength, innovation index, and market pulse assessments
    """
    logger.info("🔍 Generating final output assessment")
    
    # Extract the components from the input
    pitch_deck_data = analysis_data.get("pitch_deck_data", {})
    external_validation = analysis_data.get("external_validation", {})
    tech_product_maturity = analysis_data.get("tech_product_maturity", {})
    
    # Initialize final output structure
    final_output = {
        "Signal Strength": {"rating": "N/A", "assessment": "N/A"},
        "Innovation Index": {"rating": "N/A", "assessment": "N/A"},
        "Market Pulse": {"rating": "N/A", "assessment": "N/A"}
    }
    
    try:
        # Extract key values for context
        industry = pitch_deck_data.get("Industry", "N/A") if pitch_deck_data.get("Industry", "N/A") != "N/A" else "technology"
        startup_name = pitch_deck_data.get("Startup Name", "N/A") if pitch_deck_data.get("Startup Name", "N/A") != "N/A" else "this startup"
        business_model = pitch_deck_data.get("Business Model", "N/A") if pitch_deck_data.get("Business Model", "N/A") != "N/A" else "B2B/B2C"
        core_tech = pitch_deck_data.get("Core Technology", "N/A") if pitch_deck_data.get("Core Technology", "N/A") != "N/A" else "emerging technology"
        revenue_model = pitch_deck_data.get("Revenue Model", "N/A") if pitch_deck_data.get("Revenue Model", "N/A") != "N/A" else "subscription"
        startup_stage = pitch_deck_data.get("Startup Stage", "N/A") if pitch_deck_data.get("Startup Stage", "N/A") != "N/A" else "early-stage"
        tam = pitch_deck_data.get("TAM", "N/A") if pitch_deck_data.get("TAM", "N/A") != "N/A" else "undisclosed"
        sam = pitch_deck_data.get("SAM", "N/A") if pitch_deck_data.get("SAM", "N/A") != "N/A" else "undisclosed"
        som = pitch_deck_data.get("SOM", "N/A") if pitch_deck_data.get("SOM", "N/A") != "N/A" else "undisclosed"
        burn_rate = pitch_deck_data.get("Burn Rate", "N/A") if pitch_deck_data.get("Burn Rate", "N/A") != "N/A" else "not disclosed"
        projected_revenue = pitch_deck_data.get("Projected 12M Revenue", "N/A") if pitch_deck_data.get("Projected 12M Revenue", "N/A") != "N/A" else "not disclosed"
        
        # Get excerpts from previous analyses
        def get_excerpt(text, max_length=200):
            if text == "N/A" or not text:
                return "Not available"
            return text[:max_length] + "..." if len(text) > max_length else text
        
        market_validation = get_excerpt(external_validation.get("Market Size Validation", {}).get("summary", "N/A"))
        competitive_landscape = get_excerpt(external_validation.get("Competitive Landscape", {}).get("summary", "N/A"))
        failure_patterns = get_excerpt(external_validation.get("Industry Failure Patterns", {}).get("summary", "N/A"))
        revenue_model_comparison = get_excerpt(external_validation.get("Revenue Model Comparison", {}).get("summary", "N/A"))
        
        tech_adoption = get_excerpt(tech_product_maturity.get("AI Adoption in Industry", {}).get("summary", "N/A"))
        tech_stack = get_excerpt(tech_product_maturity.get("Tech Stack Comparison", {}).get("summary", "N/A"))
        product_readiness = get_excerpt(tech_product_maturity.get("Product Readiness", {}).get("summary", "N/A"))
        
        # Create prompt for final assessment
        final_assessment_prompt = f"""
        You are an expert venture capital analyst making a final assessment of a startup investment opportunity.
        Based on the comprehensive analysis of {startup_name} in the {industry} industry, provide a final output assessment.

        STARTUP INFORMATION:
        - Industry: {industry}
        - Business Model: {business_model}
        - Core Technology: {core_tech}
        - Revenue Model: {revenue_model}
        - Startup Stage: {startup_stage}
        - Market Size (TAM/SAM/SOM): {tam}/{sam}/{som}
        - Burn Rate: {burn_rate}
        - Projected 12M Revenue: {projected_revenue}

        VALIDATION SUMMARIES:
        - Market Validation: {market_validation}
        - Competitive Landscape: {competitive_landscape}
        - Industry Failure Patterns: {failure_patterns}
        - Revenue Model Assessment: {revenue_model_comparison}
        - Tech Adoption Analysis: {tech_adoption}
        - Tech Stack Assessment: {tech_stack}
        - Product Readiness Evaluation: {product_readiness}

        Based on this information, provide three key investment assessment metrics:

        1. SIGNAL STRENGTH (Credibility Check): 
           Rate as "High", "Moderate", or "Low" - Assess the credibility of the startup's claims, consistency of information, and alignment with industry benchmarks.

        2. INNOVATION INDEX (Tech Maturity): 
           Rate as "High", "Moderate", or "Low" - Evaluate the technological differentiation, maturity of the solution, and alignment with industry technology trends.

        3. MARKET PULSE (Business Viability): 
           Rate as "High", "Moderate", or "Low" - Assess market opportunity, competitive positioning, and overall business model viability.

        For each metric, provide:
        - A clear rating (High/Moderate/Low)
        - A detailed 150-200 word assessment explaining the rating, citing specific factors from the analysis

        Format your response ONLY as a valid JSON object with this exact structure:
        {{
          "Signal Strength": {{
            "rating": "High/Moderate/Low",
            "assessment": "150-200 word assessment here"
          }},
          "Innovation Index": {{
            "rating": "High/Moderate/Low",
            "assessment": "150-200 word assessment here"
          }},
          "Market Pulse": {{
            "rating": "High/Moderate/Low",
            "assessment": "150-200 word assessment here"
          }}
        }}

        Ensure your response is valid JSON that can be parsed directly. Do not include explanations or additional text.
        """
        
        # Generate the final assessment
        assessment_response = llm.invoke(final_assessment_prompt)
        assessment_content = assessment_response.content if hasattr(assessment_response, 'content') else str(assessment_response)
        
        # Clean and parse JSON
        cleaned_content = clean_json_content(assessment_content)
        assessment_data = json.loads(cleaned_content)
        
        # Update final output with generated assessment
        for key in final_output:
            if key in assessment_data:
                if "rating" in assessment_data[key]:
                    final_output[key]["rating"] = assessment_data[key]["rating"]
                if "assessment" in assessment_data[key]:
                    final_output[key]["assessment"] = assessment_data[key]["assessment"]
    
    except Exception as e:
        logger.error(f"Error generating final output assessment: {str(e)}")
        
        # Try individual assessments if the combined approach fails
        for assessment_area in final_output.keys():
            try:
                individual_prompt = f"""
                You are a venture capital analyst assessing {startup_name}, a {industry} startup.
                
                Based on these details:
                - Business Model: {business_model}
                - Core Technology: {core_tech}
                - Revenue Model: {revenue_model}
                - Market Size: {tam}
                
                Provide an assessment of the startup's {assessment_area}.
                
                Rate it as "High", "Moderate", or "Low" and provide a 150-200 word explanation.
                
                Format as JSON:
                {{{{
                  "rating": "High/Moderate/Low",
                  "assessment": "your assessment here"
                }}}}
                """
                
                area_response = llm.invoke(individual_prompt)
                area_content = area_response.content if hasattr(area_response, 'content') else str(area_response)
                
                # Clean and parse JSON
                cleaned_content = clean_json_content(area_content)
                area_data = json.loads(cleaned_content)
                
                if "rating" in area_data:
                    final_output[assessment_area]["rating"] = area_data["rating"]
                if "assessment" in area_data:
                    final_output[assessment_area]["assessment"] = area_data["assessment"]
            except Exception:
                # Use final fallback prompt
                try:
                    fallback_prompt = f"""
                    Rate the {assessment_area} of a {industry} startup as "High", "Moderate", or "Low" and provide a brief explanation.
                    
                    Format as JSON:
                    {{{{
                      "rating": "High/Moderate/Low",
                      "assessment": "explanation here"
                    }}}}
                    """
                    
                    fallback_response = llm.invoke(fallback_prompt)
                    fallback_content = fallback_response.content if hasattr(fallback_response, 'content') else str(fallback_response)
                    
                    cleaned_content = clean_json_content(fallback_content)
                    fallback_data = json.loads(cleaned_content)
                    
                    if "rating" in fallback_data:
                        final_output[assessment_area]["rating"] = fallback_data["rating"]
                    if "assessment" in fallback_data:
                        final_output[assessment_area]["assessment"] = fallback_data["assessment"]
                except Exception:
                    # Last resort prompt with double-escaped braces
                    last_prompt = f"""
                    Return only a JSON: {{{{
                      "rating": "High/Moderate/Low", 
                      "assessment": "Brief assessment of {assessment_area} for a {industry} startup"
                    }}}}
                    """
                    
                    try:
                        last_response = llm.invoke(last_prompt)
                        last_content = last_response.content if hasattr(last_response, 'content') else str(last_response)
                        last_data = json.loads(clean_json_content(last_content))
                        
                        rating = last_data.get("rating", "Moderate")
                        assessment = last_data.get("assessment", f"Assessment of {assessment_area} for a {industry} startup.")
                        
                        final_output[assessment_area]["rating"] = rating
                        final_output[assessment_area]["assessment"] = assessment
                    except Exception:
                        # Absolute last fallback
                        final_output[assessment_area]["rating"] = "Moderate"
                        final_output[assessment_area]["assessment"] = f"Based on the available information, {assessment_area} for this {industry} startup is assessed as Moderate."
    
    return final_output

    from openai import ChatOpenAI

def process_pitch_deck(pitch_id_input):
    """Function to process the pitch deck and return structured output."""
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # Ensure API key is handled securely
    llm = ChatOpenAI(
        api_key=OPENAI_API_KEY,
        model="gpt-4o",
        temperature=0.3
    )

    pitch_text = get_extracted_text_from_db(pitch_id_input)
    result = get_extracted_text(pitch_text)
    print(f"Extracted Text Processing Result: {result}")

    # Ensure result is stored as JSON string in the DB
    if isinstance(result, dict):
        result_json = json.dumps(result)  # Convert dict to string before saving
    else:
        result_json = result  # Keep it as is if already string

    # Insert result into ExternalValidationAgent column
    save_pitch_deck_data(pitch_id_input, external_validation=result_json)

    # ✅ Convert JSON string back to dictionary before passing to `generate_final_output`
    try:
        result_dict = json.loads(result_json) if isinstance(result_json, str) else result_json
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        result_dict = {}

    final_output = generate_final_output(result_dict, llm)  # Pass dictionary
    print(f"Final Pitch Deck Output: {final_output}")

    # Ensure final_output is stored as JSON string in the DB
    if isinstance(final_output, dict):
        final_output_json = json.dumps(final_output)  
    else:
        final_output_json = final_output  

    # Insert final output into finalPitchDeckAgent column
    save_pitch_deck_data(pitch_id_input, final_pitch_deck=final_output_json)

    return final_output_json  # Return JSON string to keep it consistent

def ensure_columns_exist():
    """Ensures ExternalValidationAgent and finalPitchDeckAgent columns exist in the pitch_deck table."""
    db_config = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT")
    }

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # First, add columns if they don’t exist
        cursor.execute("""
            ALTER TABLE pitch_decks
            ADD COLUMN IF NOT EXISTS ExternalValidationAgent JSONB,
            ADD COLUMN IF NOT EXISTS finalPitchDeckAgent JSONB;
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Columns ensured in pitch_deck table.")
    
    except Exception as e:
        print(f"Error ensuring columns exist: {e}")


def save_pitch_deck_data(pitch_id, external_validation=None, final_pitch_deck=None):
    """Updates the ExternalValidationAgent and finalPitchDeckAgent columns in the pitch_deck table."""
    db_config = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT")
    }
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Convert to JSON if it's a dictionary
        if isinstance(external_validation, dict):
            external_validation = json.dumps(external_validation)
        if isinstance(final_pitch_deck, dict):
            final_pitch_deck = json.dumps(final_pitch_deck)

        query = """
        UPDATE pitch_decks
        SET 
            ExternalValidationAgent = COALESCE(%s, ExternalValidationAgent),
            finalPitchDeckAgent = COALESCE(%s, finalPitchDeckAgent)
        WHERE pitch_id = %s;
        """
        cursor.execute(query, (external_validation, final_pitch_deck, pitch_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully updated pitch_id {pitch_id}")
    
    except Exception as e:
        print(f"Error updating database: {e}")


if __name__ == "__main__":
    pitch_id_input = input("Enter the pitch deck ID to process: ")
    ensure_columns_exist()  # Ensure columns exist before processing
    print(process_pitch_deck(pitch_id_input))
