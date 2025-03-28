import os
import sys
from crewai import Agent, Task, Crew, Process
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
import logging
import importlib.util

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Dynamically import the process_pitch_deck function
def import_from_path(module_name, file_path):
    """
    Dynamically import a module from a specific file path
    
    Args:
        module_name (str): Name to give the imported module
        file_path (str): Full path to the Python file
    
    Returns:
        The imported module
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Construct the full path to the module
current_dir = os.path.dirname(os.path.abspath(__file__))
module_path = os.path.join(current_dir, '4_updatedLinks_kruti.py')

try:
    # Dynamically import the module
    imported_module = import_from_path('updatedLinks_kruti', module_path)
    process_pitch_deck = imported_module.process_pitch_deck
except Exception as e:
    logger.error(f"Error importing process_pitch_deck: {e}")
    raise

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.error("OpenAI API key is not set!")
    raise ValueError("OPENAI_API_KEY environment variable is required")

class PitchDeckAnalysisCrew:
    def __init__(self, pitch_id, llm=None):
        """
        Initialize the Pitch Deck Analysis Crew
        
        Args:
            pitch_id (str): The ID of the pitch deck to process
            llm: Optional Language Model. Defaults to GPT-4o if not provided.
        """
        self.pitch_id = pitch_id
        
        # Use provided LLM or create a default one
        self.llm = llm or ChatOpenAI(
            api_key=OPENAI_API_KEY,
            model="gpt-4o",
            temperature=0.2
        )
        
        # Create agents
        self.pitch_analysis_agent = self._create_pitch_analysis_agent()
        
        # Create task
        self.pitch_processing_task = self._create_pitch_processing_task()
    
    def _create_pitch_analysis_agent(self):
        """
        Create an agent for pitch deck analysis and interpretation
        """
        return Agent(
            role='Pitch Deck Insights Specialist',
            goal='Provide comprehensive interpretation and strategic insights from pitch deck analysis',
            backstory='An expert venture capital analyst skilled at transforming raw pitch deck data '
                      'into actionable intelligence and strategic recommendations',
            verbose=True,
            llm=self.llm,
            allow_delegation=False
        )
    
    def _create_pitch_processing_task(self):
        """
        Create a task for processing and interpreting the pitch deck
        """
        return Task(
            description=f'''Process and analyze the pitch deck with ID: {self.pitch_id}
            
            Task Steps:
            1. Call the process_pitch_deck function with the provided pitch ID
            2. Thoroughly review the generated analysis
            3. Synthesize key insights and strategic implications
            4. Prepare a comprehensive summary of the findings''',
            agent=self.pitch_analysis_agent,
            expected_output='''A detailed report including:
            - Summary of key findings
            - Strategic implications
            - Investment potential assessment
            - Recommended next steps'''
        )
    
    def analyze_pitch_deck(self):
        """
        Analyze the pitch deck using the process_pitch_deck function
        
        Returns:
            dict: Comprehensive pitch deck analysis and insights
        """
        try:
            # Step 1: Process the pitch deck using the existing function
            pitch_analysis = process_pitch_deck(self.pitch_id)
            
            # Create a crew to provide additional context and insights
            crew = Crew(
                agents=[self.pitch_analysis_agent],
                tasks=[self.pitch_processing_task],
                process=Process.sequential,
                verbose=True
            )
            
            # Kickoff the crew with the processed data
            crew_insights = crew.kickoff(inputs={
                'pitch_id': self.pitch_id,
                'pitch_analysis': pitch_analysis
            })
            
            # Return the original analysis along with crew insights
            return {
                'pitch_id': self.pitch_id,
                'pitch_analysis': pitch_analysis,
                'crew_insights': crew_insights
            }
        
        except Exception as e:
            logger.error(f"Error analyzing pitch deck: {str(e)}", exc_info=True)
            return {
                'pitch_id': self.pitch_id,
                'error': str(e)
            }

def main():
    # Example usage
    try:
        # Get pitch deck ID from user or system
        pitch_id = input("Enter the Pitch Deck ID to analyze: ")
        
        # Initialize and analyze pitch deck
        pitch_analyzer = PitchDeckAnalysisCrew(pitch_id)
        result = pitch_analyzer.analyze_pitch_deck()
        
        # Print or further process the result
        # print("Pitch Deck Analysis Result:")
        #print(result)
    
    except Exception as e:
        logger.error(f"Pitch deck analysis failed: {str(e)}")

if __name__ == "__main__":
    main()