# test_vector_search.py
"""
Comprehensive testing script for the Vector Search functionality.
Run this script to test document upload, search, and query augmentation.
"""

import requests
import json
import os
import time
from io import BytesIO
from pathlib import Path

# Configuration
BASE_URL = "http://localhost:8000"
COMPANY_NUMBER = "CN101741403"
USER_ID = "test_user_123"

# Headers
HEADERS = {
    "company-number": COMPANY_NUMBER,
    "user-id": USER_ID,
    "Content-Type": "application/json"
}

def create_test_documents():
    """Create test documents for uploading"""
    
    # Create test directory
    test_dir = Path("test_documents")
    test_dir.mkdir(exist_ok=True)
    
    # Create a company-specific document
    company_doc = """
    COMPANY EARTHQUAKE RISK ASSESSMENT POLICY
    
    This document outlines the earthquake risk assessment procedures for all company properties.
    
    ASSESSMENT REQUIREMENTS:
    1. All properties in seismic zones 3-4 must undergo annual structural assessments
    2. Properties built before 1980 require additional retrofitting analysis
    3. Critical facilities (data centers, manufacturing) need quarterly inspections
    
    RISK MITIGATION STRATEGIES:
    - Install seismic monitoring equipment
    - Implement base isolation systems for critical structures
    - Maintain emergency response protocols
    - Regular training for facility managers
    
    REPORTING REQUIREMENTS:
    All assessment reports must be submitted to the Risk Management department within 30 days.
    Include detailed analysis of:
    - Structural integrity
    - Soil composition
    - Proximity to fault lines
    - Building code compliance
    
    INSURANCE IMPLICATIONS:
    Properties with high earthquake risk ratings may require additional coverage or higher deductibles.
    Coordinate with insurance team for policy adjustments.
    """
    
    with open(test_dir / "company_earthquake_policy.txt", "w") as f:
        f.write(company_doc)
    
    # Create a general knowledge document
    general_doc = """
    PROPERTY INSURANCE FUNDAMENTALS
    
    Understanding COPE Attributes:
    
    CONSTRUCTION:
    - Frame: Wood frame construction, higher fire risk
    - Joisted Masonry: Masonry walls with wood floors
    - Non-Combustible: Steel frame with fire-resistant materials
    - Masonry Non-Combustible: Concrete or masonry construction
    - Modified Fire Resistive: Steel frame with partial fire protection
    - Fire Resistive: Complete fire protection systems
    
    OCCUPANCY:
    - Habitational: Residential properties
    - Office: Commercial office buildings
    - Mercantile: Retail and shopping centers
    - Manufacturing: Industrial facilities
    - Institutional: Schools, hospitals, government buildings
    
    PROTECTION:
    - Sprinkler systems reduce fire risk significantly
    - Central station monitoring provides rapid response
    - Fire department proximity affects response time
    - Water supply adequacy is crucial for firefighting
    
    EXPOSURE:
    - External fire exposure from adjacent properties
    - Wildfire exposure in high-risk areas
    - Flood exposure in low-lying areas
    - Wind exposure in coastal regions
    
    NATURAL HAZARD ZONES:
    - Earthquake zones rated 0-4 (4 being highest risk)
    - Hurricane zones vary by coastal proximity
    - Tornado zones concentrated in central US
    - Flood zones designated by FEMA (100-year, 500-year)
    
    LOSS ESTIMATION:
    - Annual Average Loss (AAL): Expected yearly loss amount
    - Probable Maximum Loss (PML): Worst-case scenario loss
    - Return Period: Frequency of specific loss events
    """
    
    with open(test_dir / "insurance_fundamentals.txt", "w") as f:
        f.write(general_doc)
    
    print("✅ Test documents created in test_documents/ directory")
    return test_dir

def test_document_upload():
    """Test document upload functionality"""
    print("\n🔄 Testing Document Upload...")
    
    test_dir = create_test_documents()
    
    # Test 1: Upload company-specific document
    print("📤 Uploading company-specific document...")
    
    with open(test_dir / "company_earthquake_policy.txt", "rb") as f:
        files = {"file": ("company_earthquake_policy.txt", f, "text/plain")}
        data = {"document_type": "company_specific"}
        
        response = requests.post(
            f"{BASE_URL}/api/documents/upload",
            headers={
                "company-number": COMPANY_NUMBER,
                "user-id": USER_ID
            },
            files=files,
            data=data
        )
    
    if response.status_code == 200:
        result = response.json()
        print(f"✅ Company document uploaded: {result['doc_id']}")
        print(f"   - Filename: {result['filename']}")
        print(f"   - Chunks: {result['chunk_count']}")
        company_doc_id = result['doc_id']
    else:
        print(f"❌ Company document upload failed: {response.text}")
        return None
    
    # Test 2: Upload general document
    print("📤 Uploading general knowledge document...")
    
    with open(test_dir / "insurance_fundamentals.txt", "rb") as f:
        files = {"file": ("insurance_fundamentals.txt", f, "text/plain")}
        data = {"document_type": "general"}
        
        response = requests.post(
            f"{BASE_URL}/api/documents/upload",
            headers={
                "company-number": COMPANY_NUMBER,
                "user-id": USER_ID
            },
            files=files,
            data=data
        )
    
    if response.status_code == 200:
        result = response.json()
        print(f"✅ General document uploaded: {result['doc_id']}")
        print(f"   - Filename: {result['filename']}")
        print(f"   - Chunks: {result['chunk_count']}")
        general_doc_id = result['doc_id']
    else:
        print(f"❌ General document upload failed: {response.text}")
        return None
    
    return company_doc_id, general_doc_id

def test_document_listing():
    """Test document listing functionality"""
    print("\n🔄 Testing Document Listing...")
    
    response = requests.get(
        f"{BASE_URL}/api/documents",
        headers=HEADERS
    )
    
    if response.status_code == 200:
        documents = response.json()
        print(f"✅ Found {len(documents)} documents:")
        for doc in documents:
            print(f"   - {doc['filename']} ({doc['document_type']}) - {doc['chunk_count']} chunks")
    else:
        print(f"❌ Document listing failed: {response.text}")

def test_document_search():
    """Test document search functionality"""
    print("\n🔄 Testing Document Search...")
    
    # Test various search queries
    search_queries = [
        "earthquake risk assessment procedures",
        "COPE attributes in insurance",
        "seismic zones and building requirements",
        "construction types and fire risk"
    ]
    
    for query in search_queries:
        print(f"\n🔍 Searching for: '{query}'")
        
        search_data = {
            "query": query,
            "top_k": 3,
            "similarity_threshold": 0.5
        }
        
        response = requests.post(
            f"{BASE_URL}/api/documents/search",
            headers=HEADERS,
            json=search_data
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Found {result['total_found']} relevant chunks:")
            for i, chunk in enumerate(result['results'], 1):
                print(f"   {i}. Score: {chunk['score']:.3f}")
                print(f"      Doc Type: {chunk['metadata']['document_type']}")
                print(f"      Company: {chunk['metadata'].get('company_number', 'General')}")
        else:
            print(f"❌ Search failed: {response.text}")

def test_enhanced_query():
    """Test enhanced query processing with document context"""
    print("\n🔄 Testing Enhanced Query Processing...")
    
    # Test queries that should trigger document context
    test_queries = [
        "What are the earthquake assessment procedures for high-risk properties?",
        "Explain COPE attributes for property insurance",
        "How should I assess construction types for fire risk?",
        "What are the requirements for properties in seismic zone 4?"
    ]
    
    for question in test_queries:
        print(f"\n❓ Question: '{question}'")
        
        query_data = {
            "question": question
        }
        
        response = requests.post(
            f"{BASE_URL}/api/query",
            headers=HEADERS,
            json=query_data
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Response Type: {result['response_type']}")
            
            # Check if document context is included
            explanation = result.get('explanation', '')
            if '📚 **Relevant Information from Company Documents:**' in explanation:
                print("✅ Document context successfully included!")
                # Show first part of explanation
                context_part = explanation.split('---')[0]
                print(f"📄 Context preview: {context_part[:200]}...")
            else:
                print("ℹ️  No document context found (may be expected for SQL queries)")
            
            print(f"📋 Summary: {result.get('summary', 'No summary')[:100]}...")
        else:
            print(f"❌ Query failed: {response.text}")

def test_document_stats():
    """Test document statistics endpoint"""
    print("\n🔄 Testing Document Statistics...")
    
    response = requests.get(
        f"{BASE_URL}/api/documents/stats",
        headers=HEADERS
    )
    
    if response.status_code == 200:
        stats = response.json()
        print("✅ Document Statistics:")
        print(f"   Total Documents: {stats['totals']['total_documents']}")
        print(f"   Total Chunks: {stats['totals']['total_chunks']}")
        print(f"   Total Size: {stats['totals']['total_size_mb']} MB")
        
        print("   By Type:")
        for doc_type, type_stats in stats['by_type'].items():
            print(f"     {doc_type}: {type_stats['document_count']} docs, {type_stats['chunk_count']} chunks")
    else:
        print(f"❌ Stats retrieval failed: {response.text}")

def test_error_cases():
    """Test error handling"""
    print("\n🔄 Testing Error Cases...")
    
    # Test 1: Upload unsupported file type
    print("🚫 Testing unsupported file upload...")
    
    files = {"file": ("test.exe", b"fake executable content", "application/octet-stream")}
    data = {"document_type": "general"}
    
    response = requests.post(
        f"{BASE_URL}/api/documents/upload",
        headers={
            "company-number": COMPANY_NUMBER,
            "user-id": USER_ID
        },
        files=files,
        data=data
    )
    
    if response.status_code == 400:
        print("✅ Unsupported file type correctly rejected")
    else:
        print(f"❌ Expected 400 error, got {response.status_code}")
    
    # Test 2: Delete non-existent document
    print("🚫 Testing non-existent document deletion...")
    
    response = requests.delete(
        f"{BASE_URL}/api/documents/fake-doc-id",
        headers=HEADERS
    )
    
    if response.status_code == 404:
        print("✅ Non-existent document deletion correctly handled")
    else:
        print(f"❌ Expected 404 error, got {response.status_code}")

def cleanup_test_documents():
    """Clean up test documents (optional)"""
    print("\n🔄 Cleaning up test documents...")
    
    # Get list of documents
    response = requests.get(
        f"{BASE_URL}/api/documents",
        headers=HEADERS
    )
    
    if response.status_code == 200:
        documents = response.json()
        
        for doc in documents:
            if doc['filename'] in ['company_earthquake_policy.txt', 'insurance_fundamentals.txt']:
                print(f"🗑️  Deleting: {doc['filename']}")
                
                delete_response = requests.delete(
                    f"{BASE_URL}/api/documents/{doc['doc_id']}",
                    headers=HEADERS
                )
                
                if delete_response.status_code == 200:
                    print(f"✅ Deleted: {doc['filename']}")
                else:
                    print(f"❌ Failed to delete: {doc['filename']}")

def main():
    """Run all tests"""
    print("🚀 Starting Vector Search Integration Tests")
    print(f"Base URL: {BASE_URL}")
    print(f"Company: {COMPANY_NUMBER}")
    print(f"User: {USER_ID}")
    
    try:
        # Test document upload
        doc_ids = test_document_upload()
        if not doc_ids:
            print("❌ Document upload failed, stopping tests")
            return
        
        # Wait a moment for processing
        print("\n⏳ Waiting for document processing...")
        time.sleep(2)
        
        # Test document listing
        test_document_listing()
        
        # Test document search
        test_document_search()
        
        # Test enhanced query processing
        test_enhanced_query()
        
        # Test document statistics
        test_document_stats()
        
        # Test error cases
        test_error_cases()
        
        print("\n✅ All tests completed!")
        
        # Ask if user wants to cleanup
        cleanup = input("\n🗑️  Clean up test documents? (y/n): ").lower().strip()
        if cleanup == 'y':
            cleanup_test_documents()
    
    except requests.exceptions.ConnectionError:
        print(f"❌ Could not connect to {BASE_URL}")
        print("   Make sure the server is running: python run.py")
    
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()