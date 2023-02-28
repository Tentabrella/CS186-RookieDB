package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null | lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // The current lock type can effectively substitute the requested type
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }

        // The current lock type is IX and the requested lock is S
        if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
            lockContext.promote(transaction, LockType.SIX);
            return;
        }

        // The current lock type is an intent lock
        if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);
            explicitLockType = lockContext.getExplicitLockType(transaction);
            if (explicitLockType == requestType || explicitLockType == LockType.X) return;
        }

        // Make sure all ancestors have proper lock
        ensureParentLockHeld(lockContext, requestType, transaction);
        // grant or promote: after steps before we ensure explicitLockType is NL, S or X.
        if (explicitLockType.equals(LockType.NL)) {
            // if no lock before: grant it
            lockContext.acquire(transaction, requestType);
        } else {
            // have lock, promote
            lockContext.promote(transaction, requestType);
        }
        return;
    }

    /**
     * This method will make sure parent lockContext will hold enough lock to ensure explicit
     * requestLockType on lockContext. This method assume when first call, effective
     * lockType of lockContext can not satisfy requestType. So not possible requestType
     * is S/IS while ancestor hold S or X.
     * @param lockContext
     * @param requestType
     * @param transaction
     */
    private static void ensureParentLockHeld(LockContext lockContext, LockType requestType, TransactionContext transaction) {

        LockContext parentContext = lockContext.parentContext();
        // if no parent
        if (parentContext == null) {
            return;
        }
        LockType parentLockType = parentContext.getExplicitLockType(transaction);
        // if ok, end
        if (LockType.canBeParentLock(parentLockType, requestType)) {
            return;
        }
        // else, find need type, then update lock
        LockType targetParentLockType = parentLockType;
        if (LockType.parentLock(requestType).equals(LockType.IS)) {
            if (parentLockType.equals(LockType.NL)) {
                targetParentLockType = LockType.IS;
            }
        } else if (LockType.parentLock(requestType).equals(LockType.IX)) {
            if (parentLockType.equals(LockType.NL)) {
                targetParentLockType = LockType.IX;
            } else if (parentLockType.equals(LockType.S)) {
                targetParentLockType = LockType.SIX;
            } else if (parentLockType.equals(LockType.IS)) {
                targetParentLockType = LockType.IX;
            }
        }
//        // Do nothing if requestType can be meet (this line can be removed)
//        if (targetParentLockType.equals(parentLockType)) {
//            return;
//        }
        ensureParentLockHeld(parentContext, targetParentLockType, transaction);
        if (parentLockType.equals(LockType.NL)) {
            parentContext.acquire(transaction, targetParentLockType);
        } else {
            parentContext.promote(transaction, targetParentLockType);
        }
    }
}
