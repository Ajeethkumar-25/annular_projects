from datetime import datetime
import psycopg2
import logging
import os
import sys
from typing import Dict, Optional, Any, List
from crewai import Agent, Task, Crew
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
import json

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Check for OpenAI API Key and provide clear error if missing
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.error("⚠️ OPENAI_API_KEY environment variable is not set!")
    logger.error("Please set your OpenAI API key in .env file or environment variables")
    logger.error("Example: OPENAI_API_KEY=sk-...")
    sys.exit(1)

# PostgreSQL Configuration - secure connection details from environment variables
db_config = {
    "dbname": os.getenv("DB_NAME", "investor_db"),
    "user": os.getenv("DB_USER", "investor_user"),
    "password": os.getenv("DB_PASSWORD", "investor_password"),
    "host": os.getenv("DB_HOST", "100.26.81.244"),
    "port": int(os.getenv("DB_PORT", "5432"))
}

# Function to fetch investor data from PostgreSQL
def fetch_investor_data(investor_id: int) -> Optional[Dict[str, Any]]:
    """
    Fetch investor data from PostgreSQL database using investor_id
    """
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Using a query to get basic investor data
        query = """
        SELECT 
            investor_id, 
            investor_name, 
            email,
            linkedin_url,
            company_name,
            investor_type, 
            investment_experience, 
            investment_focus_areas,
            primary_impact_areas,
            geographical_preferences,
            startup_stages,
            business_models,
            target_roi,
            exit_horizon,
            check_size_min,
            check_size_max,
            preferred_ownership,
            revenue_milestones,
            monthly_recurring_revenue,
            tam,
            som,
            sam,
            traction_revenue_market,
            technology_scalability,
            background,
            past_investments
        FROM investor_profiles
        WHERE investor_id = %s
        """
        
        cursor.execute(query, (investor_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            columns = [
                "investor_id", "investor_name", "email", "linkedin_url", "company_name", 
                "investor_type", "investment_experience", "investment_focus_areas", "primary_impact_areas", 
                "geographical_preferences", "startup_stages", "business_models", "target_roi", 
                "exit_horizon", "check_size_min", "check_size_max", "preferred_ownership", 
                "revenue_milestones", "monthly_recurring_revenue", "tam", "som", "sam", 
                "traction_revenue_market", "technology_scalability", "background", "past_investments"
            ]
            return dict(zip(columns, result))
        else:
            logger.warning(f"No investor data found for ID: {investor_id}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Database Error: {str(e)}", exc_info=True)
        return None

# Function to generate an investor profile using CrewAI
def generate_profile_summary(investor_id: int) -> Dict[str, Any]:
    """
    Generate a 500-word structured investor profile summary using CrewAI
    and save it to the pitch_decks table
    """
    logger.info(f"🔥 Investor Profile Summary Agent started for investor ID: {investor_id}")

    # Fetch investor data from database
    investor_data = fetch_investor_data(investor_id)
    if not investor_data:
        logger.error(f"Failed to retrieve data for investor ID: {investor_id}")
        return {"error": "Investor data not found."}

    # Format the prompt with available investor data and specify 500 words
    formatted_prompt = f"""
    Create a comprehensive investor profile summary based on the following details:
    
    **Investor Type & Background**
    - **Investor Type:** {investor_data.get("investor_type", "Unknown")}
    - **Name:** {investor_data.get("investor_name", "Unknown")}
    - **Investment Experience:** {investor_data.get("investment_experience", "Unknown")}
    - **Previous Background:** {investor_data.get("background", "Unknown")}

    **Investment Thesis (Mission & Impact-Driven Focus)**
    - **Primary Impact Areas:** {investor_data.get("primary_impact_areas", "Unknown")}
    - **Geographical Preferences:**  {investor_data.get("geographical_preferences", "Unknown")}
    - **Startup Stages:** {investor_data.get("startup_stages", "Unknown")} 
    - **Business Models:** {investor_data.get("business_models", "Unknown")}
    - **Return Expectations:** {investor_data.get("target_roi", "Unknown")}
    - **Exit Horizon:** {investor_data.get("exit_horizon", "Unknown")}

    **Investment Preferences & Criteria**
    - **Check Size & Equity Target:** {investor_data.get("check_size_min", "Unknown")}
    - **Maximum Check Size:** {investor_data.get("check_size_max", "Unknown")}
    - **Preferred Ownership Stake:** {investor_data.get("preferred_ownership", "Unknown")}
    - **Traction & Growth Metrics:** {investor_data.get("revenue_milestones", "Unknown")}
    - **Monthly Recurring Revenue:** {investor_data.get("monthly_recurring_revenue", "Unknown")}
    - **Traction Revenue Market:** {investor_data.get("traction_revenue_market", "Unknown")}
    - **TAM:** {investor_data.get("tam", "Unknown")}
    - **SAM:** {investor_data.get("sam", "Unknown")}
    - **SOM:** {investor_data.get("som", "Unknown")}
    - **Technology & Scalability Preferences:** {investor_data.get("technology_scalability", "Unknown")}
    
    Your task is to:
   
    1. Format the output in a structured, markdown-friendly format
    2. Make educated inferences based on the investor's background and type
    3. Keep the summary EXACTLY 500 words - this is a strict requirement
    
    The summary should include the following sections:
    - Overview (brief introduction of the investor)
    - Investors area of Interest/Expertise
    
    Make the profile engaging, insightful, and actionable for entrepreneurs seeking funding.
    """

    try:
        # Define the Investor Profile Analyst agent with specific output constraints
        profile_analyst = Agent(
            role="Investor Profile Analyst",
            goal="Create a precise 500-word investor profile summary",
            backstory="""You are an experienced investment professional specializing in
            investor profiling and analysis. You have extensive knowledge of the venture
            capital ecosystem and understand the nuances of different investor types,
            investment strategies, and decision-making processes. You are known for your
            ability to distill complex information into concise, actionable summaries.""",
            llm=ChatOpenAI(
                api_key=OPENAI_API_KEY,
                model="gpt-4",
                temperature=0.2
            ),
            verbose=True,
            allow_delegation=False
        )

        # Define the task for generating the investor profile with word count constraint
        profile_task = Task(
            description=formatted_prompt,
            agent=profile_analyst,
            expected_output="A precisely 500-word structured investor profile summary with insights and recommendations"
        )

        # Create the crew and kickoff the task
        crew = Crew(
            agents=[profile_analyst],
            tasks=[profile_task],
            verbose=True,
            process="sequential"
        )
        
        # Run the crew and get the profile summary
        print(f"🤖 Running Investor Profile Analysis for investor ID {investor_id}...")
        crew_output = crew.kickoff()
        
        # Extract the actual text from the CrewOutput object
        if hasattr(crew_output, 'raw'):
            profile_summary = crew_output.raw
        elif isinstance(crew_output, list) and len(crew_output) > 0:
            profile_summary = crew_output[0]
        elif hasattr(crew_output, 'result'):
            profile_summary = crew_output.result
        elif hasattr(crew_output, 'outputs') and len(crew_output.outputs) > 0:
            profile_summary = crew_output.outputs[0]
        elif str(crew_output):
            profile_summary = str(crew_output)
        else:
            logger.error(f"Unknown CrewOutput format: {type(crew_output)}")
            return {"error": "Unable to extract summary from CrewAI output"}
        
        # Now that we have the text, count words
        word_count = len(str(profile_summary).split())
        logger.info(f"Generated summary with {word_count} words")
        
        # Structure the response
        structured_profile = {
            "investor_id": investor_id,
            "full_text": profile_summary,
            "structured_data": investor_data,
            "word_count": word_count
        }
        
        logger.info(f"✅ Investor profile summary generated for Investor ID: {investor_id}")
        
        # Save to database
        save_success = save_investor_summary(investor_id, profile_summary)
        if not save_success:
            logger.error(f"Failed to save summary for investor {investor_id}")
            structured_profile["save_status"] = "failed"
        else:
            structured_profile["save_status"] = "success"
            logger.info(f"✅ Profile summary saved to database for investor ID: {investor_id}")
        
        return structured_profile
        
    except Exception as e:
        import traceback
        logger.error(f"Error generating profile: {str(e)}")
        logger.error(traceback.format_exc())
        return {"error": f"Failed to generate profile: {str(e)}"}

# Function to handle database updates
def save_investor_summary(investor_id: int, summary_text: str) -> bool:
    """
    Save the generated investor summary to the investor_profiles table
    """
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Ensure the column exists
        cursor.execute("""
        ALTER TABLE investor_profiles ADD COLUMN IF NOT EXISTS "investorSettingsAgent" JSONB;
        """)
        
        # Create a structured JSON object
        summary_json = json.dumps({
            "investor_id": investor_id,
            "summary": summary_text,
            "timestamp": datetime.now().isoformat()
        })
        
        # Update the summary for the investor
        update_query = """
        UPDATE investor_profiles
        SET "investorSettingsAgent" = %s::jsonb
        WHERE investor_id = %s
        """
        cursor.execute(update_query, (summary_json, investor_id))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    
    except Exception as e:
        logger.error(f"Database error: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
        return False

# Main function for command-line usage
def main(investor_id: int):
    """Main function for command-line usage"""
    print(f"🔍 Generating a 500-word summary for Investor ID: {investor_id}...")
    
    # Generate the profile summary
    summary = generate_profile_summary(investor_id)
    
    # Display the results
    if "error" in summary:
        print(f"\n❌ Error: {summary['error']}")
        return False
    else:
        print(f"\n✅ Profile Summary Generated for Investor ID: {investor_id}")
        print(f"Word Count: {summary.get('word_count', 'unknown')}")
        print(f"Save Status: {summary.get('save_status', 'unknown')}")
        
        print("\n--- SUMMARY START ---\n")
        print(summary["full_text"])
        print("\n--- SUMMARY END ---\n")
        
        return True

# Command-line entry point
if __name__ == "__main__":
    # If no investor ID was provided as a command-line argument, prompt for it
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        investor_id = int(sys.argv[1])
    else:
        try:
            investor_id = int(input("Enter investor ID: "))
        except ValueError:
            print("⚠️ Error: Investor ID must be a number")
            sys.exit(1)
    
    success = main(investor_id)
    sys.exit(0 if success else 1)