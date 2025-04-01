import os
import sys
import time
import logging
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import json
import importlib.util
import inspect

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crewai_orchestrator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AgentOrchestrator:
    """
    Orchestrates the execution of multiple CrewAI agents with parallel execution
    for the first two agents and sequential execution for the remaining ones.
    """
    
    def __init__(self):
        """Initialize the orchestrator with agent configurations"""
        self.agent_configs = [
            # Step 1: Run in parallel
            {
                "name": "Investment Settings Agent",
                "filename": "inestment_settings_agent.py",
                "function_name": "generate_profile_summary",
                "parallel": True,
                "input_key": "investor_id",
                "output_key": "investor_settings"
            },
            {
                "name": "Pitch Deck Processing Agent",
                "filename": "pitch_deck_processing_agent.py",
                "function_name": "process_pitch_deck",
                "parallel": True,
                "input_key": "pitch_id",
                "output_key": "pitch_processing"
            },
            
            # Step 2: Run sequentially after step 1
            {
                "name": "Thesis Matching Agent",
                "filename": "thesis_matching_agent.py",
                "function_name": "generate_investor_thesis_matching",
                "parallel": False,
                "input_key": "pitch_id",
                "output_key": "thesis_matching"
            },
            
            # Step 3: Run sequentially after step 2
            {
                "name": "Investment Summary Agent",
                "filename": "investment_summary_agent.py",
                "function_name": "generate_executive_summary",
                "parallel": False,
                "input_key": "pitch_id",
                "output_key": "investment_summary"
            }
        ]
        
        # Verify all agent modules exist
        for config in self.agent_configs:
            module_path = os.path.join(os.getcwd(), config["filename"])
            if not os.path.exists(module_path):
                logger.error(f"Agent module not found: {module_path}")
                raise FileNotFoundError(f"Agent module not found: {config['filename']}")
        
        logger.info("All agent modules verified")
    
    def _import_module_from_file(self, file_path):
        """
        Dynamically import a module from a file path
        
        Args:
            file_path (str): Path to the Python file
        
        Returns:
            module: Imported module
        """
        module_name = os.path.splitext(os.path.basename(file_path))[0]
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    
    def _run_module_function(self, config: Dict[str, Any], context: Dict[str, Any], specific_input=None):
        """
        Run a specific function from a module
        
        Args:
            config (dict): Configuration for the module and function
            context (dict): Context dictionary containing input parameters
            specific_input (any, optional): Specific input to override context-based input
        
        Returns:
            The result of the function call
        """
        try:
            # Construct full file path
            module_path = os.path.join(os.getcwd(), config["filename"])
            
            # Dynamically import the module
            module = self._import_module_from_file(module_path)
            
            # Get the function
            func = getattr(module, config["function_name"])
            
            # Determine input value
            if specific_input is not None:
                # If specific input is a dictionary with thesis matching results, extract pitch_id
                if isinstance(specific_input, dict) and 'ThesisMatching' in specific_input:
                    input_value = context.get("pitch_id")
                else:
                    input_value = specific_input
            else:
                input_value = context.get(config["input_key"])
            
            if input_value is None:
                logger.error(f"No input value found for {config['input_key']}")
                return None
            
            # Call the function
            logger.info(f"Executing {config['name']} with input: {input_value}")
            result = func(input_value)
            
            # Log result summary for debugging
            result_summary = self._generate_result_summary(result)
            logger.info(f"{config['name']} result summary: {result_summary}")
            
            return result
        
        except Exception as e:
            logger.error(f"Error executing {config['name']}: {str(e)}")
            raise
    
    def _generate_result_summary(self, result):
        """Generate a concise summary of a result for logging purposes"""
        if result is None:
            return "None"
        
        if isinstance(result, dict):
            keys = list(result.keys())
            return f"Dict with {len(keys)} keys: {', '.join(keys[:5])}"
        
        if isinstance(result, str):
            if len(result) > 100:
                return f"String ({len(result)} chars): {result[:100]}..."
            return f"String: {result}"
        
        return f"Type: {type(result)}"
    
    def run_parallel_agents(self, parallel_configs: List[Dict[str, Any]], context: Dict[str, Any]):
        """
        Run multiple agents in parallel
        
        Args:
            parallel_configs: List of agent configurations to run in parallel
            context: Shared context dictionary for agent outputs
            
        Returns:
            bool: True if all agents succeeded, False otherwise
        """
        logger.info(f"Starting parallel execution of {len(parallel_configs)} agents")
        
        try:
            with ThreadPoolExecutor(max_workers=len(parallel_configs)) as executor:
                # Submit tasks
                future_to_config = {
                    executor.submit(self._run_module_function, config, context): config 
                    for config in parallel_configs
                }
                
                # Collect results
                for future in as_completed(future_to_config):
                    config = future_to_config[future]
                    try:
                        result = future.result()
                        context[config["output_key"]] = result
                        logger.info(f"{config['name']} completed successfully")
                    except Exception as e:
                        logger.error(f"{config['name']} failed: {str(e)}")
                        return False
            
            return True
        
        except Exception as e:
            logger.error(f"Parallel execution error: {str(e)}")
            return False
    
    def run_sequential_agents(self, sequential_configs: List[Dict[str, Any]], context: Dict[str, Any]):
        """
        Run agents sequentially, passing outputs as inputs to subsequent agents
        
        Args:
            sequential_configs: List of agent configurations to run sequentially
            context: Shared context dictionary for agent outputs
            
        Returns:
            tuple: (success_flag, final_result)
        """
        # Track the last agent's output for input to the next agent
        last_output = None
        final_result = None
        
        for i, config in enumerate(sequential_configs, start=1):
            logger.info(f"Step {i}: Running {config['name']} sequentially")
            
            try:
                # If it's the first sequential agent, use context-based input
                # For subsequent agents, use the previous agent's output as additional context
                if last_output is not None and config["name"] != "Investment Summary Agent":
                    # Pass previous output as specific input only for Thesis Matching Agent
                    result = self._run_module_function(config, context, specific_input=last_output)
                else:
                    # For Investment Summary Agent, just use the pitch_id from context
                    result = self._run_module_function(config, context)
                
                # Store the result in context
                context[config["output_key"]] = result
                
                # Update last_output for next iteration
                last_output = result
                
                # Store the final result from the last agent
                if i == len(sequential_configs):
                    final_result = result
                
                logger.info(f"{config['name']} completed successfully")
            except Exception as e:
                logger.error(f"Sequential execution of {config['name']} failed: {str(e)}")
                return False, None
        
        # Combine results into a comprehensive final output
        combined_result = self.combine_results(context)
        
        return True, combined_result
    
    def combine_results(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Combine results from all agents into a comprehensive output
        that fully preserves the Investment Summary Agent's complete output.
        
        Args:
            context: Dictionary containing all agent outputs
        
        Returns:
            dict: Combined results with all key sections
        """
        # Get the investment summary as the primary source
        investment_summary = context.get("investment_summary", {})
        
        # If investment_summary is a string, try to parse it as JSON
        if isinstance(investment_summary, str):
            try:
                investment_summary = json.loads(investment_summary)
            except json.JSONDecodeError:
                # If parsing fails, wrap it in a basic structure
                investment_summary = {
                    "Executive Summary": "Investment Summary",
                    "Summary Text": investment_summary
                }
        
        # If investment_summary is empty or None, create a placeholder
        if not investment_summary:
            investment_summary = {
                "Executive Summary": "No detailed summary available"
            }
        
        # Create the combined result, starting with the complete investment_summary
        combined_result = investment_summary.copy()
        
        # Add thesis matching data if available, but don't overwrite existing data
        thesis_matching = context.get("thesis_matching", {})
        if thesis_matching and isinstance(thesis_matching, dict):
            # Add ThesisMatching if it doesn't already exist in combined_result
            if "ThesisMatching" not in combined_result and "ThesisMatching" in thesis_matching:
                combined_result["ThesisMatching"] = thesis_matching["ThesisMatching"]
            
            # Add InvestmentSummary if it doesn't already exist in combined_result
            if "InvestmentSummary" not in combined_result and "InvestmentSummary" in thesis_matching:
                combined_result["InvestmentSummary"] = thesis_matching["InvestmentSummary"]
            
            # Add FinalInvestmentMatchAnalysis if it doesn't already exist
            if "FinalInvestmentMatchAnalysis" not in combined_result and "FinalInvestmentMatchAnalysis" in thesis_matching:
                combined_result["FinalInvestmentMatchAnalysis"] = thesis_matching["FinalInvestmentMatchAnalysis"]
        
        # Add pitch processing data if available and not already present
        pitch_processing = context.get("pitch_processing", {})
        if pitch_processing and isinstance(pitch_processing, dict):
            # Add a PitchAnalysis section if not already present
            if "PitchAnalysis" not in combined_result:
                # Try to extract key details from pitch processing
                pitch_analysis = {}
                
                # Check different potential structures
                if "pitch_deck_data" in pitch_processing:
                    pitch_analysis = pitch_processing["pitch_deck_data"]
                elif "Signal Strength" in pitch_processing:
                    pitch_analysis["Signal Strength"] = pitch_processing["Signal Strength"]
                elif "Innovation Index" in pitch_processing:
                    pitch_analysis["Innovation Index"] = pitch_processing["Innovation Index"]
                elif "Market Pulse" in pitch_processing:
                    pitch_analysis["Market Pulse"] = pitch_processing["Market Pulse"]
                
                if pitch_analysis:
                    combined_result["PitchAnalysis"] = pitch_analysis
        
        # Add investor settings data if available
        investor_settings = context.get("investor_settings", {})
        if investor_settings and isinstance(investor_settings, dict):
            if "InvestorProfile" not in combined_result:
                # Extract relevant investor data
                investor_profile = {}
                
                if "investor_id" in investor_settings:
                    investor_profile["investor_id"] = investor_settings["investor_id"]
                
                if "structured_data" in investor_settings:
                    investor_profile["preferences"] = investor_settings["structured_data"]
                elif "full_text" in investor_settings:
                    investor_profile["summary"] = investor_settings["full_text"]
                
                if investor_profile:
                    combined_result["InvestorProfile"] = investor_profile
        
        # Add standardized result metadata
        combined_result["metadata"] = {
            "processing_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "agents_used": [
                "Investment Settings Agent",
                "Pitch Deck Processing Agent",
                "Thesis Matching Agent",
                "Investment Summary Agent"
            ]
        }
        
        return combined_result
    
    def orchestrate(self, pitch_id: int, investor_id: int):
        """
        Orchestrate the execution of all agents in the correct order
        
        Args:
            pitch_id: The pitch deck ID to process
            investor_id: The investor ID to use
            
        Returns:
            tuple: (success_flag, final_result)
        """
        context = {
            "pitch_id": pitch_id,
            "investor_id": investor_id
        }
        logger.info("Starting orchestration process")
        
        # Step 1: Identify which agents to run in parallel
        parallel_configs = [config for config in self.agent_configs if config["parallel"]]
        
        if parallel_configs:
            logger.info(f"Step 1: Running {len(parallel_configs)} agents in parallel")
            parallel_success = self.run_parallel_agents(parallel_configs, context)
            
            if not parallel_success:
                logger.error("Parallel execution failed, aborting orchestration")
                return False, None
            
            # Log the current context keys after parallel execution
            logger.info(f"Context after parallel execution: {list(context.keys())}")
        
        # Steps 2+: Run remaining agents sequentially
        sequential_configs = [config for config in self.agent_configs if not config["parallel"]]
        
        if sequential_configs:
            logger.info(f"Step 2: Running {len(sequential_configs)} agents sequentially")
            sequential_success, final_result = self.run_sequential_agents(sequential_configs, context)
            
            if not sequential_success:
                logger.error("Sequential execution failed, aborting orchestration")
                return False, None
            
            # Log the keys in the final result
            if isinstance(final_result, dict):
                logger.info(f"Final result keys: {list(final_result.keys())}")
            else:
                logger.info(f"Final result type: {type(final_result)}")
        
        logger.info("Orchestration completed successfully")
        return True, final_result


if __name__ == "__main__":
    # Get pitch ID and investor ID from command line or user input
    if len(sys.argv) > 2:
        try:
            pitch_id = int(sys.argv[1])
            investor_id = int(sys.argv[2])
        except ValueError:
            print("Error: Both pitch ID and investor ID must be integers")
            sys.exit(1)
    else:
        try:
            pitch_id = int(input("Enter the pitch deck ID to process: "))
            investor_id = int(input("Enter the investor ID to use: "))
        except ValueError:
            print("Error: Both pitch ID and investor ID must be integers")
            sys.exit(1)
    
    print(f"\n{'='*50}")
    print(f" Starting CrewAI Investment Analysis Pipeline ")
    print(f" Pitch ID: {pitch_id}, Investor ID: {investor_id} ")
    print(f"{'='*50}\n")
    
    # Create the orchestrator
    orchestrator = AgentOrchestrator()
    
    # Run the full sequence
    start_time = time.time()
    
    try:
        success, final_result = orchestrator.orchestrate(pitch_id, investor_id)
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        if success:
            print(f"\n{'='*50}")
            print(f" ✅ Successfully completed all processing ")
            print(f" Total execution time: {total_duration:.2f} seconds ")
            
            # Clean print of final result
            if isinstance(final_result, dict):
                # Check if it's an error result
                if 'error' in final_result:
                    print("\n Error in processing:")
                    print(final_result['error'])
                else:
                    print("\n Final Investment Summary:")
                    
                    # First print the main executive summary content if available
                    if "Executive Summary" in final_result:
                        print(f"\n--- Executive Summary ---")
                        if isinstance(final_result['Executive Summary'], dict):
                            print(json.dumps(final_result['Executive Summary'], indent=2))
                        else:
                            print(final_result['Executive Summary'])
                    
                    # Print the complete detailed analysis sections
                    detailed_sections = ['Signal Strength', 'Innovation Index', 'Market Pulse', 'Thesis Fit Score', 'Final Recommendation']
                    for section in detailed_sections:
                        if section in final_result:
                            print(f"\n--- {section} ---")
                            if isinstance(final_result[section], dict):
                                print(json.dumps(final_result[section], indent=2))
                            else:
                                print(final_result[section])
                    
                    # Then print the thesis matching data
                    supplementary_sections = ['ThesisMatching', 'InvestmentSummary', 'FinalInvestmentMatchAnalysis']
                    for section in supplementary_sections:
                        if section in final_result:
                            print(f"\n--- {section} ---")
                            print(json.dumps(final_result[section], indent=2))
            else:
                print("\n Final Result:")
                print(final_result)
            
            print(f"\n{'='*50}\n")
        else:
            print(f"\n{'='*50}")
            print(f" ❌ Errors occurred during processing ")
            print(f" Check logs for details ")
            print(f"{'='*50}\n")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
        sys.exit(1)
    
    except Exception as e:
        print(f"\n{'='*50}")
        print(f" ❌ Unexpected error: {str(e)} ")
        print(f"{'='*50}\n")
        logger.error(f"Orchestration failed with unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)