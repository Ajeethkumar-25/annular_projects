import os
import sys
import time
import logging
from typing import Dict, Any, List
from crewai import Crew, Process, Task
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
            
            return result
        
        except Exception as e:
            logger.error(f"Error executing {config['name']}: {str(e)}")
            raise
    
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
        
        # Use ThreadPoolExecutor for parallel execution
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
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
            bool: True if all agents succeeded, False otherwise
        """
        # Track the last agent's output for input to the next agent
        last_output = None
        
        for i, config in enumerate(sequential_configs, start=1):
            logger.info(f"Step {i}: Running {config['name']} sequentially")
            
            try:
                # If it's the first sequential agent, use context-based input
                # For subsequent agents, use the previous agent's output
                if last_output is not None:
                    result = self._run_module_function(config, context, specific_input=last_output)
                else:
                    result = self._run_module_function(config, context)
                
                # Store the result in context
                context[config["output_key"]] = result
                
                # Update last_output for next iteration
                last_output = result
                
                logger.info(f"{config['name']} completed successfully")
            except Exception as e:
                logger.error(f"Sequential execution of {config['name']} failed: {str(e)}")
                return False
        
        return True, last_output
    
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
        
        # Steps 2+: Run remaining agents sequentially
        sequential_configs = [config for config in self.agent_configs if not config["parallel"]]
        
        if sequential_configs:
            logger.info(f"Running {len(sequential_configs)} agents sequentially")
            sequential_success, final_result = self.run_sequential_agents(sequential_configs, context)
            
            if not sequential_success:
                logger.error("Sequential execution failed, aborting orchestration")
                return False, None
        
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
                    # Highlight key sections
                    sections = ['ThesisMatching', 'InvestmentSummary', 'FinalInvestmentMatchAnalysis']
                    for section in sections:
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