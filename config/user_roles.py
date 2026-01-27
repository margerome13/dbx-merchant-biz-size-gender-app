"""
User Role Configuration for Merchant Business Size and Gender Review App

This file defines which users have which roles.
Roles: ADMIN, MAKER, CHECKER

- ADMIN: Can switch between roles and see all interfaces
- MAKER: Can only submit reviews
- CHECKER: Can only approve/reject reviews
"""

# Admin users - can switch roles and access everything
ADMINS = [
    "mar.abana@paymaya.com"
    # Add more admin emails here
]

# Maker users - can submit reviews
MAKERS = [
    "revylen.asilo@paymaya.com",
    "revylen.asilo@maya.ph",
    # Add more maker emails here
]

# Checker users - can approve/reject reviews
CHECKERS = [
    ""
    # Add more checker emails here
]

def get_user_role(user_email: str) -> str:
    """
    Determine user role based on email address.
    
    Args:
        user_email: The user's email address
        
    Returns:
        "ADMIN", "MAKER", "CHECKER", or "UNAUTHORIZED"
    """
    # Normalize email to lowercase for comparison
    email = user_email.lower().strip()
    
    # Check admin first (highest privilege)
    if email in [admin.lower() for admin in ADMINS]:
        return "ADMIN"
    
    # Check maker
    if email in [maker.lower() for maker in MAKERS]:
        return "MAKER"
    
    # Check checker
    if email in [checker.lower() for checker in CHECKERS]:
        return "CHECKER"
    
    # User not in any list
    return "UNAUTHORIZED"

def is_admin(user_email: str) -> bool:
    """Check if user is an admin"""
    return get_user_role(user_email) == "ADMIN"

def is_maker(user_email: str) -> bool:
    """Check if user is a maker"""
    role = get_user_role(user_email)
    return role in ["ADMIN", "MAKER"]

def is_checker(user_email: str) -> bool:
    """Check if user is a checker"""
    role = get_user_role(user_email)
    return role in ["ADMIN", "CHECKER"]
